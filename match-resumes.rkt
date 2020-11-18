#lang racket
; copyright (c) 2020 Roy E Lowrance, all rights reserved
(require csv-reading)
(require debug)
(require racket/hash)

(define options
  (let ((project-path (lambda (p) (build-path "../.." p))))
    (hash
     'fraction-resume-retained 1.0
     'n-occupation-matches 3
     'n-task-matches 5
     'n-job-matches 10
     'path-to-jobs-dir (project-path "input/from-andres/nyu-project/jobs")
     'path-to-occupations-file (project-path "input/from-uk-gov/occupational-codes/soc2020volume1descriptionofunitgroups270820/SOC2020_Volume1_description_of unit_groups_27-08-20.csv")
     'path-to-resumes-dir (project-path "work/input-data/resumes")
     'stopword-cutoff-frequency 0.002
   )))

; string number (hash/c word Int)
; count each downcased word in a string (creating a bag of words)
(define (string->bow s fraction-retained); -> (hash/c word count)
  (define verbose #f)
  (define all-words (regexp-match* #px"[A-Za-z][A-Za-z0-9-]*" s))
  (define to-select (inexact->exact (round (* (length all-words) fraction-retained))))
  (when (and verbose (not (equal? (length all-words) to-select)))
    (printf "used the first ~a words of ~a in ~a~n" to-select (length all-words) s))
  (define words (take all-words to-select))
  (let iter ((words words)
             (result (hash)))
    (cond ((null? words) result)
          (else (iter (cdr words)
                      (hash-set result
                                (string-downcase (car words))
                                (add1 (hash-ref result (car words) 0))))))))

; hash hash hash
(define (hash-combine bow1 bow2)
  (hash-union bow1 bow2 #:combine (lambda (v1 v2) (+ v1 v2))))

; path (values (hash/c string bow) (hash/c string bow))
; for each group name, return two bow's: one for the occupations, one for the tasks
(define (read-occupation-file path); -> (values occupations-by-grouptitle tasks-by-grouptitle)
  (define verbose #f)
  (define unit-group-index 3)
  (define group-title-index 4)
  (define group-description-index 6)
  (define tasks-index 8)
  (define related-job-titles-index 9)
  (define (extract record-number xs occupations-by-grouptitle tasks-by-grouptitle)
    (when verbose
      (for (((grouptitle bow) occupations-by-grouptitle))
        (printf "grouptitle ~a has ~a unique occupation words~n" grouptitle (hash-count bow))))
    (cond ((equal? record-number 1) (extract (add1 record-number) (cdr xs) occupations-by-grouptitle tasks-by-grouptitle)); skip comment
          ((equal? record-number 2) (extract (add1 record-number) (cdr xs) occupations-by-grouptitle tasks-by-grouptitle)); skip column names
          ;((equal? record-number 10) (values occupations-by-grouptitle tasks-by-grouptitle)); stop early when debugging
          ((null? xs) (values occupations-by-grouptitle tasks-by-grouptitle))
          (else (let* ((x (first xs))
                       (unit-group (list-ref x unit-group-index))
                       (group-title (list-ref x group-title-index))
                       (occupations-words (string-append (list-ref x group-title-index)
                                                         (list-ref x group-description-index)
                                                         (list-ref x tasks-index)
                                                         (list-ref x related-job-titles-index)))
                       (tasks-words (list-ref x tasks-index)))
                  (when verbose (printf "unit-group:~a group-title:~a~n" unit-group group-title))
                  (cond
                    ((equal? unit-group "<blank>") (extract (add1 record-number) (cdr xs) occupations-by-grouptitle tasks-by-grouptitle))
                    ((equal? tasks-words "<blank>") (extract (add1 record-number) (cdr xs) occupations-by-grouptitle tasks-by-grouptitle))
                    (else (extract (add1 record-number)
                                   (cdr xs)
                                   (hash-set occupations-by-grouptitle group-title (string->bow occupations-words 1.0))
                                   (hash-set tasks-by-grouptitle group-title (string->bow tasks-words 1.0)))))))))
  #;(define path (string-append (hash-ref options 'path-to-occupations-dir)
                              "/"
                              (hash-ref options 'occupation-file-name)))
  (define reader (make-csv-reader (open-input-file path)))
  (define rows (csv->list reader))
  (extract 1 rows (hash) (hash)))

; (hash/c _ number) number
; how many words in total?
(define (bow-total-count bow)
  (foldl + 0 (hash-values bow)))

; (hash/c string number)
(define (bow-print bow)
  (define sorted-words (sort (hash-keys bow) (lambda (a b) (< (hash-ref bow a) (hash-ref bow b)))))
  (for ((word sorted-words))
    (printf "~a ~a~n" (hash-ref bow word) word)))

; (hash/c string (hash/c string number)) (hash/c string number)
(define (accumulate-counts occupations-by-grouptitle);-> (hash/c word count)
  (let iter ((group-titles (hash-keys occupations-by-grouptitle))
             (result (hash)))
    (cond ((null? group-titles) result)
          (else (iter (cdr group-titles)
                      (hash-union result
                                  (hash-ref occupations-by-grouptitle (car group-titles))
                                  #:combine/key (lambda (word count1 count2) (+ count1 count2))))))))

; hash hash hash
; set difference between two hashes
(define (hash-difference h0 h); return hash containing items in h0 that are not in h
  (for/hash (((k v) h0)
             #:unless (hash-has-key? h k))
    (values k v)))

; (hashc/ string number) number (hash/c string number)
; drop most frequent words in the vocabulary
(define (make-stopwords vocabulary cutoff-frequency); -> (hash/c word 1)
  (define verbose #f)
  ;(when verbose (for (((word count) vocabulary)) (printf "count:~a word:~a~n" count word)))
  (define unsorted-words (for/list (((word count) vocabulary))
                           (cons word count)))
  (define sorted-words (sort unsorted-words (lambda (a b) (< (cdr a) (cdr b)))))
  (define total-count (foldl + 0 (map cdr sorted-words)))
  (define h (for/hash ((word-count sorted-words))
              (let ((word (car word-count))
                    (count (cdr word-count)))
                (define frequency (/ count total-count))
                (define included (> frequency cutoff-frequency))
                (when verbose (printf "~a ~a ~a~n" count (exact->inexact frequency) word))
                (values word frequency))))
  (for/hash (((word frequency) h)
             #:when (> frequency cutoff-frequency))
    (values word 1)))

; path string
; read file at path into a string
(define (read-file-at-path path); -> string
  (let ((port (open-input-file path #:mode 'text)))
    (port->string port #:close? #t)))

; path (hash/c string string)
; read all visible files in a directory into a string per filename
(define (read-dir-files path-to-dir); -> (hash/c filename-path string)
  ; path path boolean
  ; is the path to a file that is not hidden?
  (define (fso-ok? local-path)
    (and (file-exists? (build-path path-to-dir local-path))
         (not (equal? #\. (string-ref (path->string local-path) 0)))))
  (for/hash ((fso (directory-list path-to-dir))
             #:when (fso-ok? fso))
    (values fso (read-file-at-path (build-path path-to-dir fso)))))

; string number (hash/c string (hash/c string number))
; read all visible files in a directory as a bag of words
(define (dir->bags-of-words path retained); -> (hash/c filename bow)
  (define files (read-dir-files path))
  (for/hash (((filename s) files))
    (values filename (string->bow s retained))))

; string number (hash/c string (hash/c string number))
; read all the jobs
#;(define (read-jobs path-to-dir fraction-retained)
  (define files (read-dir-files (hash-ref options 'path-to-jobs)))
  (for/hash (((file-name s) files))
    (values file-name (string->bow s 1.0))))
                                                      
; string number (values string (hash/c string number))
; read all of the resumes
#;(define (read-resumes path-to-dir fraction-retained); (hash/c file-name bow)
  (define files
    (read-dir-files path-to-dir))
  (for/hash (((file-name s) files))
    (values file-name (string->bow s fraction-resume-retained))))
    
; (hash/c word number) (hash/c word number) number
; angle between vectors (in range [-1,1]
(define (cosine-similarity a b)
  (define (dot-product a b); dot product of corresponding values
    (define keys-a (hash-keys a))
    (define values-a (hash-values a))
    (define values-b (map (lambda (k) (hash-ref b k 0)) keys-a))
    (define product (map * values-a values-b))
    (foldl + 0 product))
  (define (square x) (expt x 2))
  (define (magnitude h)
    (sqrt (foldl + 0 (map square (hash-values h)))))
  (let ((dp (dot-product a b))
        (normalizer (* (magnitude a) (magnitude b))))
    (cond ((zero? normalizer) 0)
          (else (/ dp normalizer)))))

; (hash/c string bow) (hash/c string bow) (hash/c string (listof string.number))
; match each resume to files in a target hash
(define (match-resumes resumes targets); -> (hash/c resume-filename (listof/sorted target-key.cosine-similarity))
  (define (match-resume resume-filename resume-bow); match to all targets
    (printf "match-resume ~a resume-bow~n" resume-filename)
    (bow-print resume-bow)
    (define unsorted (for/list (((target-filename target-bow) targets))
                       (printf "match-resumes resume:~a target:~a cosine-similarity:~a~n" resume-filename target-filename (cosine-similarity resume-bow target-bow))
                       (cons target-filename (cosine-similarity resume-bow target-bow))))
    (reverse (sort unsorted (lambda (a b) (< (cdr a) (cdr b))))))
  (for/hash (((resume-filename resume-bow) resumes))
    (values resume-filename (match-resume resume-filename resume-bow))))

; (hash/c string bow) (hash/c string bow) (hash/c string (listof string.number))
; rank order the jobs for each resume
#;(define (match-resumes-to-jobs resumes jobs)
  (define (match-resume resume-filename)
    (let ((unsorted (for/list ((job-filename (hash-keys jobs)))
                      (printf "resume-filename:~a job-filename:~a~n" resume-filename job-filename)
                      (cons job-filename
                            (cosine-similarity (hash-ref resumes resume-filename)
                                               (hash-ref jobs job-filename))))))
      (reverse (sort unsorted (lambda (a b) (< (cdr a) (cdr b)))))))
  (for/hash ((resume-filename (hash-keys resumes)))
    (values resume-filename (match-resume resume-filename))))

; (hash/c name resume-bow) (hash/c occupation-name tasks-bow) (hash/c name (sorted-listof (occupation-name.similarity-score))
; rank order the occupation names for each person
#;(define (match-resumest-to-occupations resumes occupations-by-grouptitle)
  (define (match-resume resume-file-name); -> sorted list of occupations (closest match to worst)
    (define resume (hash-ref resumes resume-file-name))
    (let ((unsorted (for/list (((group-title group-description) occupations-by-grouptitle))
                      (cons group-title (cosine-similarity resume group-desciption)
                      (cons group-title
                            (cosine-similarity (hash-ref resumes resume-file-name)
                                               (hash-ref occupations-by-grouptitle group-title))))))
      (reverse (sort unsorted (lambda (a b) (< (cdr a) (cdr b)))))))
  (for/hash ((resume-file-name (hash-keys resumes)))
    (values resume-file-name
            (match-resume resume-file-name)))))

; n list
(define (til n)
  (build-list n (lambda (x) x)))

; (listof string) (listof string)
(define (string-sort s)
  (sort s (lambda (a b) (string<? a b))))

; query-grouptitle tasks-by-grouptitle (listof grouptitle similarity-score)
(define (match-tasks query-grouptitle tasks-by-grouptitle); -> (sorted-listof grouptitle similarity-score)
  (define query-bow (hash-ref tasks-by-grouptitle query-grouptitle))
  (let ((unsorted (for/list (((other-grouptitle other-bow) tasks-by-grouptitle))
                    (cons other-grouptitle
                          (cosine-similarity query-bow other-bow)))))
    (reverse (sort unsorted (lambda (a b) (< (cdr a) (cdr b)))))))

; (hash/c symbol any)
(define (print-options options)
  (printf "options~n")
  (for ((k  (sort (hash-keys options)
                  (lambda (a b) (symbol<? a b)))))
    (printf " ~a:~a~n" k (hash-ref options k)))
  (printf "~n"))

; (hash/c string bow) (hash/c string bow) (hash/c string bow) bow
(define (accumulate-vocabulary a b c)
  (define (check-is-bow x)
    (let iter ((ks (hash-keys x))
               (vs (hash-values x)))
      (cond ((null? ks) x)
            ((not (string? (car ks))) (error 'check-is-bow "key ~a is not a string" (car ks)))
            ((not (number? (car vs))) (error 'check-is-bow "value ~a is not a number" (car vs)))
            (else (iter (cdr ks) (cdr vs))))))
  (let iter ((bows (append (hash-values a) (hash-values b) (hash-values c)))
             (result (hash)))
    (cond ((null? bows) result)
          (else (iter (cdr bows)
                      (hash-union result (check-is-bow (car bows)) #:combine (lambda (v1 v2) (+ v1 v2))))))))

; number (listof number)
(define (go)
  (print-options options)
  ; build vocabulary so as to be able to derive stopwords
  (define-values (all-occupations all-tasks) (read-occupation-file (hash-ref options 'path-to-occupations-file)))
  (for (((grouptitle bow) all-occupations)) (printf "occupation:~a has ~a words, ~a unique~n" grouptitle (bow-total-count bow) (hash-count bow)))
  
  (define all-resumes (dir->bags-of-words (hash-ref options 'path-to-resumes-dir)
                                          (hash-ref options 'fraction-resume-retained)))
  (newline)
  (for (((filename bow) all-resumes)) (printf "resume filename:~a has ~a words, ~a unique~n" filename (bow-total-count bow) (hash-count bow)))

  (define all-jobs (dir->bags-of-words (hash-ref options 'path-to-jobs-dir) 1.0))
  (newline)
  (for (((filename bow) all-jobs)) (printf "job filename:~a has ~a words, ~a unique~n" filename (bow-total-count bow) (hash-count bow)))

  (define all-vocabulary (accumulate-vocabulary all-occupations all-resumes all-jobs))
  (newline)
  (printf "the vocabulary (from the occupations, resumes, and jobs) has ~a words, ~a unique~n" (bow-total-count all-vocabulary) (hash-count all-vocabulary))

  (define stopwords (make-stopwords all-vocabulary
                                    (hash-ref options 'stopword-cutoff-frequency)))
  (printf "~nthese stopwords were excluded from the analysis below~n")
  (for ((stopword (hash-keys stopwords)))
    (printf "count in vocabulary:~a stopword:~a~n" (hash-ref all-vocabulary stopword) stopword))
    
  ; remove stopwords
  (define occupations (for/hash (((grouptitle bow) all-occupations))
                        (values grouptitle (hash-difference bow stopwords))))
  (define tasks (for/hash (((grouptitle bow) all-tasks))
                  (values grouptitle (hash-difference bow stopwords))))
  (define resumes (for/hash (((filename bow) all-resumes))
                    (values filename (hash-difference all-resumes stopwords))))
  (define jobs (for/hash (((filename bow) all-jobs))
                 (values filename (hash-difference all-resumes stopwords))))

  
  ; match each resume to occupations, then the occupations' tasks to other occupation tasks
  (define n-occupation-matches (hash-ref options 'n-occupation-matches))
  (define resume-matches (match-resumes resumes occupations))
  (for (((resume-filename sorted-occupations) resume-matches))
    (printf "~n~a closest matching group titles for resume-filename:~a~n" n-occupation-matches resume-filename)
    (for ((i (til n-occupation-matches)))
      (define x (list-ref sorted-occupations i))
      (define matching-group-title (car x))
      (define matching-occupation-similarity-score (cdr x))
      (printf "  group-title:~a, with similarity score:~a~n" matching-group-title matching-occupation-similarity-score)
      (define n-task-matches (hash-ref options 'n-task-matches))
      (printf "    ~a closest matching occupations based on the tasks for these occupations~n" n-task-matches)
      (define task-matches (match-tasks matching-group-title tasks))
      (for ((i (til n-task-matches)))
        (define x (list-ref task-matches i))
        (printf "      group-title:~a, with task similarity score:~a~n" (car x) (cdr x)))))
  ; match each resume to jobs
  (define job-matches (match-resumes resumes jobs)); -> (hash/c resume-filename (listof/sorted (job-file-name . similarity)))
  (define n-job-matches (hash-ref options 'n-job-matches))
  (printf "~n")
  (for (((resume-filename sorted-job-matches) job-matches))
    (printf "~n~a closest jobs for resume-filename:~a~n" n-job-matches resume-filename)
    (for ((i (til n-job-matches)))
      (define x (list-ref sorted-job-matches i))
      (printf " resume-filename:~a, with similarity score:~a~n" (car x) (cdr x))))
  )


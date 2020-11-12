#lang racket
(require csv-reading)
(require debug)
(require racket/hash)

(define options
  (hash
   'n-occupation-matches 3
   'n-task-matches 5
   'path-to-data  "../input-data"
   'path-to-occupations-dir  "../../input/from-uk-gov/occupational-codes/soc2020volume1descriptionofunitgroups270820"
   'stop-word-cutoff 0.03
   'occupation-file-name  "SOC2020_Volume1_description_of unit_groups_27-08-20.csv"
   'resumes-dir-name "resumes"
   ))

; string (hash/c word Int)
; count each word in a string (creating a bag of words)
(define (string->bow s); -> (hash/c word count)
  (define words (regexp-match* #px"[A-Za-z][A-Za-z0-9-]*" s))
  (let iter ((words words)
             (result (hash)))
    (cond ((null? words) result)
          (else (iter (cdr words)
                      (hash-set result (car words) (add1 (hash-ref result (car words) 0))))))))

; hash hash hash
(define (hash-combine bow1 bow2)
  (hash-union bow1 bow2 #:combine (lambda (v1 v2) (+ v1 v2))))

; hash (values (hash/c string bow) (hash/c string bow))
; for each group name, return bow for the occupations and for the tasks
(define (read-occupation-file options); -> (values occupations-by-grouptitle tasks-by-grouptitle)
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
                                   (hash-set occupations-by-grouptitle group-title (string->bow occupations-words))
                                   (hash-set tasks-by-grouptitle group-title (string->bow tasks-words)))))))))
  (define path (string-append (hash-ref options 'path-to-occupations-dir)
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

; options (values stop-words occupations-by-grouptitle tasks-by-grouptitle)
(define (handle-occupations options)
  (define verbose #f)
  (define-values
    (occupations-by-grouptitle tasks-by-grouptitle) (read-occupation-file options))
  (define vocabulary; the vocabulary for the occupations (words in resumes will be additional)
    (accumulate-counts occupations-by-grouptitle)); the occupations words include all of the tasks words
  (define vocabulary-word-count
    (foldl + 0 (hash-values vocabulary)))
  (define vocabulary-unique-word-count
    (hash-count vocabulary))
  (printf "read ~a occupations, which contained (before removing stop words) ~a words, of which ~a were unique~n"
          (hash-count occupations-by-grouptitle)
          vocabulary-word-count
          vocabulary-unique-word-count)
  (when verbose
    (printf "frequency, count, word~n")
    (let ((sorted-words (sort (hash-keys vocabulary) (lambda (w1 w2) (< (hash-ref vocabulary w1) (hash-ref vocabulary w2))))))
      (for ((word sorted-words))
        (let ((count (hash-ref vocabulary word)))
          (printf "~a ~a ~a~n"
                  (exact->inexact (/ count vocabulary-unique-word-count))
                  count
                  word)))))
  (define stop-words; (hash/c word 1)
    (let ((cutoff (hash-ref options 'stop-word-cutoff)))
      (for/hash (((word count) vocabulary)
                 #:when (> (/ count vocabulary-unique-word-count) cutoff))
        (values word 1))))
  (for ((word (sort (hash-keys stop-words) (lambda (a b) (string<? a b)))))
    (printf "stop-word:~a~n" word))
  (values stop-words
          (hash-difference occupations-by-grouptitle stop-words)
          (hash-difference tasks-by-grouptitle stop-words)))

; path (hash/c word count)
; build bag of words from file at path
(define (path->bow path)
  (define port (open-input-file path #:mode 'text))
  (define str (port->string port #:close? #t))
  (string->bow str))
    
; (hash/c symbol value) (hash/c string (hash/c word count))
; read all of the resumes
(define (read-resumes stop-words options); (hash/c file-name bag-of-words)
  (define dir (build-path (string->path (hash-ref options 'path-to-data))
                                (string->path (hash-ref options 'resumes-dir-name))))
  (define fsos (directory-list dir))
  (for/hash ((fso fsos))
    (define path-to-file (build-path dir fso))
    (when (file-exists? path-to-file)
      (values (path->string fso)
              (hash-difference (path->bow path-to-file) stop-words)))))

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
  (/ (dot-product a b)
     (* (magnitude a) (magnitude b))))

; (hash/c name resume-bow) (hash/c occupation-name tasks-bow) (hash/c name (sorted-listof (occupation-name.similarity-score))
; rank order the occupation names for each person
(define (match-resumes resumes occupations-by-grouptitle)
  (define (match-resume resume-file-name); -> sorted list of occupations (closest match to worst)
    (let ((unsorted (for/list ((group-title (hash-keys occupations-by-grouptitle)))
                      (cons group-title
                            (cosine-similarity (hash-ref resumes resume-file-name)
                                               (hash-ref occupations-by-grouptitle group-title))))))
      (reverse (sort unsorted (lambda (a b) (< (cdr a) (cdr b)))))))
  (for/hash ((resume-file-name (hash-keys resumes)))
    (values resume-file-name
            (match-resume resume-file-name))))

; n list
(define (til n)
  (build-list n (lambda (x) x)))

; query-grouptitle tasks-by-grouptitle (listof grouptitle similarity-score)
(define (match-tasks query-grouptitle tasks-by-grouptitle); -> (sorted-listof grouptitle similarity-score)
  (define query-bow (hash-ref tasks-by-grouptitle query-grouptitle))
  (let ((unsorted (for/list (((other-grouptitle other-bow) tasks-by-grouptitle))
                    (cons other-grouptitle
                          (cosine-similarity query-bow other-bow)))))
    (reverse (sort unsorted (lambda (a b) (< (cdr a) (cdr b)))))))

; number (listof number)
(define (go)
  (define-values (stop-words occupations-by-grouptitle tasks-by-grouptitle) (handle-occupations options))
  (define resumes (read-resumes stop-words options))
  (for (((file-name bow) resumes))
    (printf "resume file-name:~a has ~a words (excluding stop-words), of which ~a are unique~n"
            file-name
            (bow-total-count bow)
            (hash-count bow))
    ;(bow-print bow)
    )
  ; match each resume to occupations, then occupation tasks to other occupation tasks
  (define n-occupation-matches (hash-ref options 'n-occupation-matches))
  (define resume-matches (match-resumes resumes occupations-by-grouptitle))
  (for (((resume-filename sorted-occupations) resume-matches))
    (printf "~n~a closest matching group titles for resume-filename:~a~n" 3 resume-filename)
    (for ((i (til n-occupation-matches)))
      (define x (list-ref sorted-occupations i))
      (define matching-group-title (car x))
      (define matching-occupation-similarity-score (cdr x))
      (printf "  group-title:~a, with similarity score:~a~n" matching-group-title matching-occupation-similarity-score)
      (define n-task-matches (hash-ref options 'n-task-matches))
      (printf "    ~a closest matching occupations based on the tasks for these occupations~n" n-task-matches)
      (define task-matches (match-tasks matching-group-title tasks-by-grouptitle))
      (for ((i (til n-task-matches)))
        (define x (list-ref task-matches i))
        (printf "      group-title:~a, with task similarity score:~a~n" (car x) (cdr x)))))
  )


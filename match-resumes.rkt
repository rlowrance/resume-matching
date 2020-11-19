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
     'stopword-cutoff-frequency 0.003
   )))

; bag of words (bow)
(struct bow (value)
  #:transparent
  #:guard (lambda (value type-name)
            (define (all-strings? xs) (andmap string? xs))
            (define (all-numbers? xs) (andmap number? xs))
            (cond ((not (hash? value)) (error type-name "value not a hash: ~a" value))
                  (else (let ((keys (hash-keys value))
                              (values (hash-values value)))
                          (cond ((and (null? keys) (null? values)) value)
                                ((not (all-strings? keys)) (error type-name "keys not all strings: ~a" keys))
                                ((not (all-numbers? values)) (error type-name "values not all numbers: ~a" values))
                                (else value)))))))
(define (bow-words x) (hash-keys (bow-value x)))
(define (bow-counts x) (hash-values (bow-value x)))
(define (bow-count x) (hash-count (bow-value x)))
(define (bow-has-word? x w) (hash-has-key? (bow-value x) w))
(define bow-ref
  (case-lambda ((x k) (hash-ref (bow-value x) k))
               ((x k default-value) (hash-ref (bow-value x) k default-value))))
(define (bow-set x k v) (bow (hash-set (bow-value x) k v)))
(define (bow-total-of-counts bow); number of words in total
  (foldl + 0 (bow-counts bow)))
(define (bow-print x); print in order of count
  (define sorted-words (sort (bow-words x) (lambda (word1 word2) (< (bow-ref x word1) (bow-ref x word2)))))
  (for ((word sorted-words))
    (printf "count: ~a word: ~a~n" (bow-ref x word) word)))
(define (bow-union x1 x2); -> bow
  (define h (hash-union (bow-value x1)
                        (bow-value x2)
                        #:combine (lambda (count1 count2) (+ count1 count2))))
  (bow h))

; corpus - a collection of documents
(struct corpus (value)
  #:transparent
  #:guard (lambda (value type-name)
            (define (string-or-path? x) (or (string? x) (path? x)))
            (define (all-strings-or-paths? xs) (andmap string-or-path? xs))
            (define (all-bows? xs) (andmap bow? xs))
            (cond ((not (hash? value)) (error type-name "value not a hash: ~a" value))
                  (else (let ((keys (hash-keys value))
                              (values (hash-values value)))
                          (cond ((not (all-strings-or-paths? keys)) (error type-name "keys not all strings or paths: ~a" keys))
                                ((not (all-bows? values)) (error type-name "values not all bow: ~a" values))
                                (else value)))))))
(define (corpus-documentnames x) (hash-keys (corpus-value x)))
(define (corpus-bows x) (hash-values (corpus-value x)))
(define (corpus-count x) (hash-count (corpus-value x)))
(define (corpus-ref x k) (hash-ref (corpus-value x) k))
(define (corpus-set x k v) (corpus (hash-set (corpus-value x) k v)))
(define (corpus-vocabulary x); bow for all documents
  (let iter ((documentnames (corpus-documentnames x))
             (result (bow (hash))))
    (cond ((null? documentnames) result)
          (else (let ((documentname (car documentnames)))
                  (iter (cdr documentnames)
                        (bow-union result (corpus-ref x documentname))))))))
(define (corpus-print name x)
  (for (((k v) (corpus-value x)))
    (printf "~a document-name: ~a words: ~a unique words: ~a~n" name  k (bow-total-of-counts v) (bow-count v))))
                          
; string number bow
; count each downcased word in a string (creating a bag of words)
(define (string->bow s fraction-retained); -> (hash/c word count)
  (define verbose #f)
  (define all-words (regexp-match* #px"[A-Za-z][A-Za-z0-9-]*" s))
  (define to-select (inexact->exact (round (* (length all-words) fraction-retained))))
  (when (and verbose (not (equal? (length all-words) to-select)))
    (printf "used the first ~a words of ~a in ~a~n" to-select (length all-words) s))
  (define words (take all-words to-select))
  (let iter ((words words)
             (result (bow (hash))))
    (cond ((null? words) result)
          (else (iter (cdr words)
                      (bow-set result
                                (string-downcase (car words))
                                (add1 (bow-ref result (car words) 0))))))))

; path (values corpus corpus)
(define (read-occupation-file path); -> (values occupations-corpus tasks-corpus)
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
                                   (corpus-set occupations-by-grouptitle group-title (string->bow occupations-words 1.0))
                                   (corpus-set tasks-by-grouptitle group-title (string->bow tasks-words 1.0)))))))))
  (define reader (make-csv-reader (open-input-file path)))
  (define rows (csv->list reader))
  (extract 1 rows (corpus (hash)) (corpus (hash))))

; bow number (hash/c string 1)
; determine the frequent words in the vocabulary
(define (make-stopwords vocabulary cutoff-frequency); -> (hash/c word 1)
  (define verbose #f)
  ;(when verbose (for (((word count) vocabulary)) (printf "count:~a word:~a~n" count word)))
  (define unsorted-words (for/list (((word count) (bow-value vocabulary)))
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
; path corpus
; read all visible files in a directory as a bag of words
(define (dir->corpus path retained); -> (hash/c filename bow)
  (define files (read-dir-files path))
  (let iter ((filenames (hash-keys files))
             (result (corpus (hash))))
    (cond ((null? filenames) result)
          (else (let ((filename (car filenames)))
                  (iter (cdr filenames) (corpus-set result filename (string->bow (hash-ref files filename) retained))))))))
    
; bow bow number
; angle between vectors (in range [-1,1])
(define (cosine-similarity a b)
  (define (dot-product a b)
    (define words-a (bow-words a))
    (define counts-a (bow-counts a))
    (define counts-b (map (lambda (k) (bow-ref b k 0)) words-a))
    (define products (map * counts-a counts-b))
    (foldl + 0 products))
  (define (square x) (expt x 2))
  (define (magnitude h)
    (sqrt (foldl + 0 (map square (bow-counts h)))))
  (let ((dp (dot-product a b))
        (normalizer (* (magnitude a) (magnitude b))))
    (cond ((zero? normalizer) 0)
          (else (/ dp normalizer)))))

; bow corpus (listof/sorted string.number)
(define (match-bow-corpus query targets); -> (listof/sorted documentname.cosine-similarity)
  (define unsorted (for/list (((documentname target-bow) (corpus-value targets)))
                     (cons documentname (cosine-similarity query target-bow))))
  (reverse (sort unsorted (lambda (a b) (< (cdr a) (cdr b))))))

; corpus corpus (hash/c string (listof string.number))
; match each resume to files in a target hash
(define (match-corpus-corpus queries targets); -> (hash/c query-documentname (listof/sorted target-documentname.cosine-similarity))
  (for/hash (((query-documentname query-bow) (corpus-value queries)))
    (values query-documentname (match-bow-corpus query-bow targets))))

; n list
(define (til n)
  (build-list n (lambda (x) x)))

; query-grouptitle tasks-by-grouptitle (listof grouptitle similarity-score)
(define (match-tasks query-grouptitle tasks-by-grouptitle); -> (sorted-listof grouptitle similarity-score)
  (define query-bow (corpus-ref tasks-by-grouptitle query-grouptitle))
  (let ((unsorted (for/list (((other-grouptitle other-bow) (corpus-value tasks-by-grouptitle)))
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

; corpus corpus corpus bow
(define (accumulate-vocabulary a b c)
  (define bow-a (corpus-vocabulary a))
  (define bow-b (corpus-vocabulary b))
  (define bow-c (corpus-vocabulary c))
  (define bow-ab (bow-union bow-a bow-b))
  (define bow-abc (bow-union bow-ab bow-c))
  bow-abc)

; corpus (hash/c string 1) corpus
(define (remove-stopwords-from-corpus c stopwords); -> corpus
  (define (remove-from x); bow -> bow
    (bow (for/hash (((k v) (bow-value x))
               #:unless (hash-has-key? stopwords k))
      (values k v))))
  (corpus (for/hash (((documentname bow) (corpus-value c)))
    (values documentname (remove-from bow)))))

; MAIN
(define (go)
  (define verbose #f)
  (when verbose (print-options options))
  ; build vocabulary so as to be able to derive stopwords
  (define-values (all-occupations all-tasks) (read-occupation-file (hash-ref options 'path-to-occupations-file)))
  (define all-resumes (dir->corpus (hash-ref options 'path-to-resumes-dir)
                                   (hash-ref options 'fraction-resume-retained)))
  (define all-jobs (dir->corpus (hash-ref options 'path-to-jobs-dir) 1.0))
  (define all-vocabulary (accumulate-vocabulary all-occupations all-resumes all-jobs))
  
  (when verbose
    (newline) (corpus-print "occupations with stopwords" all-occupations)
    (newline) (corpus-print "tasks with stopwords" all-tasks)
    (newline) (corpus-print "resumes with stopwords" all-resumes)
    (newline) (corpus-print "jobs with stopwords" all-jobs))
    
  (newline)
  (printf "the vocabulary (from the occupations, tasks, resumes, and jobs) has ~a words, ~a unique~n" (bow-total-of-counts all-vocabulary) (bow-count all-vocabulary))

  (define stopword-cutoff-frequency (hash-ref options 'stopword-cutoff-frequency))
  (define stopwords (make-stopwords all-vocabulary stopword-cutoff-frequency))
  (printf "~nthese stopwords were excluded from the analysis below, as they occured more frequently than ~a~n" stopword-cutoff-frequency)
  (for ((stopword (hash-keys stopwords)))
    (printf "count in vocabulary: ~a stopword: ~a~n" (bow-ref all-vocabulary stopword) stopword))
    
  ; remove stopwords
  (define occupations (remove-stopwords-from-corpus all-occupations stopwords))
  (define tasks (remove-stopwords-from-corpus all-tasks stopwords))
  (define resumes (remove-stopwords-from-corpus all-resumes stopwords))
  (define jobs (remove-stopwords-from-corpus all-jobs stopwords))

  (when #f
    (printf "~ncorpus sizes after removing stopwords~n")
    (corpus-print "occupations" occupations)
    (corpus-print "tasks" tasks)
    (corpus-print "resumes" resumes)
    (corpus-print "jobs" jobs))

  
  ; match each resume to occupations, then the occupations' tasks to other occupation tasks
  (define n-occupation-matches (hash-ref options 'n-occupation-matches))
  (define resume-matches (match-corpus-corpus resumes occupations)); -> hash
  (for (((resume-filename sorted-occupations) resume-matches))
    (printf "~n~a closest matching group titles for resume-filename: ~a~n" n-occupation-matches resume-filename)
    (for ((i (til n-occupation-matches)))
      (define x (list-ref sorted-occupations i))
      (define matching-group-title (car x))
      (define matching-occupation-similarity-score (cdr x))
      (printf "  group-title: ~a, with similarity score: ~a~n" matching-group-title matching-occupation-similarity-score)
      (define n-task-matches (hash-ref options 'n-task-matches))
      (printf "    ~a closest matching occupations based on the tasks for these occupations~n" n-task-matches)
      (define task-matches (match-tasks matching-group-title tasks))
      (for ((i (til n-task-matches)))
        (define x (list-ref task-matches i))
        (printf "      group-title: ~a, with task similarity score:~a~n" (car x) (cdr x)))))
  ; match each resume to jobs
  (define job-matches (match-corpus-corpus resumes jobs)); -> (hash/c resume-filename (listof/sorted (job-file-name . similarity)))
  (define n-job-matches (hash-ref options 'n-job-matches))
  (printf "~n")
  (for (((resume-filename sorted-job-matches) job-matches))
    (printf "~n~a closest jobs for resume-filename:~a~n" n-job-matches resume-filename)
    (for ((i (til n-job-matches)))
      (define x (list-ref sorted-job-matches i))
      (printf " resume-filename:~a, with similarity score:~a~n" (car x) (cdr x))))
  )


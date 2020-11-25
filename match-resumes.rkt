#lang racket
; copyright (c) 2020 Roy E Lowrance, all rights reserved

(require csv-reading)
(require debug)
(require racket/hash)

(define options
  (let ((project-path (lambda (p) (build-path "../.." p))))
    (hash
     'determine-stopword-cutoff #f; oneof #t #f
     'dev #f; oneof #t #f; set to #t when developing the code
     'fraction-resume-retained 1.0
     'n-occupation-matches 3
     'n-task-matches 5
     'n-job-matches 10
     'path-to-jobs-dir (project-path "input/from-andres/nyu-project/jobs")
     'path-to-occupations-file (project-path "input/from-uk-gov/occupational-codes/soc2020volume1descriptionofunitgroups270820/SOC2020_Volume1_description_of unit_groups_27-08-20.csv")
     'path-to-resumes-dir (project-path "work/input-data/resumes")
     'path-to-stemmed-voc-file (project-path "input/from-snowball-dot-tartarus-dot-org/english/voc.txt")
     'path-to-stemmed-output-file (project-path "input/from-snowball-dot-tartarus-dot-org/english/output.txt")
     'weights-algorithm 'tf3-idf2; oneof 'tf3-idf2 'tf2-unary
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
(define (bow-weights x) (hash-values (bow-value x)))
(define (bow-count x) (hash-count (bow-value x)))
(define (bow-has-word? x w) (hash-has-key? (bow-value x) w))
(define bow-ref
  (case-lambda ((x k) (hash-ref (bow-value x) k))
               ((x k default-value) (hash-ref (bow-value x) k default-value))))
(define (bow-set x k v) (bow (hash-set (bow-value x) k v)))
(define (bow-total-of-counts bow); number of words in total
  (foldl + 0 (bow-weights bow)))
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

; document
(struct document (kind name hash); -> symbol string hash
  #:transparent
  #:guard (lambda (k n h type-name)
            (cond ((not (symbol? k)) (error type-name "kind field not a symbol: ~a" k))
                  ((not (or (path? n) (string? n))) (error type-name "name field not a path or string: ~a" n))
                  ((not (hash? h)) (error type-name "hash field not a hash: ~a" h))
                  (else (values k
                                (if (path? n) (path->string n) n)
                                h)))))
                 
(define (head-iter obj n)
  (cond ((list? obj) (reverse (let iter ((lst obj)(current-length 0)(result (list)))
                                (cond ((null? lst) result)
                                      ((equal? current-length n) result)
                                      (else (iter (cdr lst) (add1 current-length) (cons (car lst) result)))))))
        ((hash? obj) (let iter ((keys (hash-keys obj))(result (hash)))
                       (cond ((equal? n (hash-count result)) result)
                             (else (iter (cdr keys) (hash-set result (car keys) (hash-ref obj (car keys))))))))))

; (or/c list hash) [number] (or/c list hash)
(define head
  (case-lambda ((obj) (head-iter obj 3))
               ((obj n) (head-iter obj n))))
  

(define (println-head message obj)
  (writeln message)
  (writeln obj)
  (writeln (head obj))
  (printf "(head ~a): ~a~n" message (head obj)))
                          
; string number bow
; count each downcased word in a string (creating a bag of words)
(define (string->bow s fraction-retained); -> (hash/c word count)
  (define verbose #f)
  (define all-words (regexp-match* #px"[A-Za-z][A-Za-z0-9\\-']*" s))
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

; string number (hash/c string number)
(define (string->words-counts s fraction-retained); -> (hash/c word count)
  (define verbose #f)
  (define all-words (regexp-match* #px"[A-Za-z][A-Za-z0-9-]*" s))
  (define to-select (inexact->exact (round (* (length all-words) fraction-retained))))
  (when (and verbose (not (equal? (length all-words) to-select)))
    (printf "used the first ~a words of ~a in ~a~n" to-select (length all-words) s))
  (define words (take all-words to-select))
  (let iter ((words words)
             (result (hash)))
    (cond ((null? words) result)
          (else (let ((dc-word (string-downcase (car words))))
                  (iter (cdr words)
                        (hash-set result
                                  dc-word
                                  (add1 (hash-ref result dc-word 0)))))))))
          
; path dev (values corpus corpus)
(define (read-occupation-file path dev); -> (values occupations-corpus tasks-corpus)
  (define verbose #f)
  ; columns in the CSV file
  (define unit-group-index 3)
  (define group-title-index 4)
  (define group-description-index 6)
  (define tasks-index 8)
  (define related-job-titles-index 9)
  (define (extract record-number xs occupations tasks)
    (cond ((equal? record-number 1) (extract (add1 record-number) (cdr xs) occupations tasks)); skip comment
          ((equal? record-number 2) (extract (add1 record-number) (cdr xs) occupations tasks)); skip column names
          ((and dev (equal? record-number 10)) (values occupations tasks)); stop early when debugging
          ((null? xs) (values occupations tasks))
          (else (let* ((x (first xs))
                       (unit-group (list-ref x unit-group-index))
                       (group-title (list-ref x group-title-index))
                       (occupations-words (string-append (list-ref x group-title-index)
                                                         " "
                                                         (list-ref x group-description-index)
                                                         " "
                                                         (list-ref x tasks-index)
                                                         " "
                                                         (list-ref x related-job-titles-index)))
                       (tasks-words (list-ref x tasks-index)))
                  (when verbose (printf "unit-group:~a group-title:~a~n" unit-group group-title))
                  (cond
                    ((equal? unit-group "<blank>") (extract (add1 record-number) (cdr xs) occupations tasks))
                    ((equal? tasks-words "<blank>") (extract (add1 record-number) (cdr xs) occupations tasks))
                    (else (extract (add1 record-number)
                                   (cdr xs)
                                   (cons (document 'occupation group-title (string->words-counts occupations-words 1.0)) occupations)
                                   (cons (document 'task group-title (string->words-counts tasks-words 1.0)) tasks))))))))
  (define reader (make-csv-reader (open-input-file path)))
  (define rows (csv->list reader))
  (extract 1 rows (list) (list)))

; path hash
; Reading stemming file (a CSV) into a hash
(define (read-stemming-file path); (hash/c word stemmed-word)
  (define verbose #t)
  (define word-index 0)
  (define stemmed-word-index 1)
  (define (extract record-number xs result)
    (cond ((equal? record-number 1) (extract (add1 record-number)
                                             (cdr xs)
                                             result))
          (else (let* ((x (first xs))
                       (word (list-ref x word-index))
                       (stemmed-word (list-ref stemmed-word-index)))
                  (when verbose (printf "read-stemming-file ~a ~a ~a~n" x word stemmed-word))
                  (extract (add1 record-number)
                          (cdr xs)
                          (hash-set result word stemmed-word))))))
  (define reader (make-csv-reader (open-input-file path)))
  (define rows (csv->list reader))
  (extract 1 rows (hash)))

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

; path boolean (hash/c string string)
; read all visible files in a directory into a string per filename
(define (read-dir-files path-to-dir dev); -> (hash/c filename-path string)
  (define verbose #f)
  ; path path boolean
  ; is the path to a file that is not hidden?
  (define (fso-ok? local-path)
    (and (file-exists? (build-path path-to-dir local-path))
         (not (equal? #\. (string-ref (path->string local-path) 0)))))
  (define filenames (directory-list path-to-dir))
  (when verbose (printf "read-dir-files path-to-dir:~a dev:~a filenames:~a~n" path-to-dir dev filenames))
  (for/hash ((fso (if dev (head filenames) filenames))
             #:when (fso-ok? fso))
    (values fso (read-file-at-path (build-path path-to-dir fso)))))

; string number boolean (hash/c string (hash/c string number))
; path corpus
; read all visible files in a directory as a bag of words
(define (dir->corpus path retained dev); -> (hash/c filename bow)
  (define files (read-dir-files path dev)); -> (hash/c path string)
  (let iter ((filenames (hash-keys files))
             (result (corpus (hash))))
    (cond ((null? filenames) result)
          (else (let ((filename (car filenames)))
                  (iter (cdr filenames) (corpus-set result filename (string->bow (hash-ref files filename) retained))))))))

; path number string boolean list
(define (dir->documents path retained kind dev); -> (listof (document kind filename (hash/c word count)
  (define files (read-dir-files path dev)); (hash/c path-to-file string)
  (for/list (((path str) files))
    (document kind
              path
              (string->words-counts str retained))))

    
; hash hash number
; angle between vectors (in range [-1,1])
(define (cosine-similarity a b)
  (define (dot-product a b)
    (define words-a (hash-keys a))
    (define counts-a (hash-values a))
    (define counts-b (map (lambda (k) (hash-ref b k 0)) words-a))
    (define products (map * counts-a counts-b))
    (foldl + 0 products))
  (define (square x) (expt x 2))
  (define (magnitude h)
    (sqrt (foldl + 0 (map square (hash-values h)))))
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
    (printf " ~a: ~a~n" k (hash-ref options k)))
  (printf "~n"))

; corpus corpus corpus bow
(define (accumulate-vocabulary-corpuses a b c)
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

; list hash
; accumulate all of the words
(define (accumulate-vocabulary lst); -> (hash/c word count)
  (let iter ((lst lst)
             (result (hash)))
    (cond ((null? lst) result)
          (else (iter (cdr lst)
                      (hash-union result (document-hash (car lst))))))))
    
  
; (listof documents) symbol (listof documents)
; convert hash of counts in each documnts to hash of weights
(define (make-weights docs algo); -> (listof documents)
  ; transform doc's hash from count to td3-idf2 weight
  (define (tf3-idf2 doc); term frequency, inverse document frequency
    (define n-docs (length docs))
    (define counts-hash (document-hash doc))
    (define n-terms (foldl + 0 (hash-values counts-hash)))
    (define (n-terms-in-doc term doc)
      (foldl + 0 (hash-values (document-hash doc))))
    (define (n-docs-with-term term)
      (foldl + 0 (map (lambda (doc) (if (hash-has-key? (document-hash doc) term) 1 0))
                      docs)))
    (define (tf3 term doc); term-count / number of terms in doc
      (define h (document-hash doc))
      (/ (hash-ref h term) (foldl + 0 (hash-values h))))
    (define (idf2 term)
      (log (/ n-docs (n-docs-with-term term))))
    (define result (for/hash ((term (hash-keys (document-hash doc))))
                     (values term (* (tf3 term doc)
                                     (idf2 term)))))
    (document (document-kind doc)
              (document-name doc)
              result))
  (cond ((equal? algo 'tf3-idf2) (map tf3-idf2 docs))
        ((equal? algo 'tf2-unary) docs); use the raw counts
        (else (error 'make-weights "unknown algo: ~a" algo))))

; list hash
; accumulate the hashes in a list of documents
(define (documents-hash-union docs); -> (hash/c word number)
  (define (union h1 h2) (hash-union h1 h2 #:combine (lambda (v1 v2) (+ v1 v2))))
  (foldl union (hash) (map document-hash docs)))

; hash list
; sort hash keys by value
(define (hash-keys-sorted-by-value h)
  (define sorted (sort (for/list (((k v) h)) (cons k v))
                       (lambda (pair1 pair2) (< (cdr pair1) (cdr pair2)))))
  (map car sorted))

; hash hash hash
; difference of hashs
(define (hash-difference h1 h2); -> hash of elements in h1 that are not in h2
  (for/hash (((k1 v1) h1)
             #:unless (hash-has-key? h2 k1))
    (values k1 v1)))
    
; symbol (listof document) (listof document)
; keep only documents of the specified kind
(define (filter-documents kind lst)
  (for/list ((doc lst))
    (when (equal? kind (document-kind doc))
      doc)))

; list hash hash list
; remove stopwords, then stem each document in a list
(define (remove-stopwords-then-stem documents stemming-table stopwords); -> (listof (document))
  (define (remove-stopwords h)
    (for/hash (((word x) h)
          #:unless (hash-has-key? stopwords word))
      (values word x)))
  (define (stem h)
    (for/hash (((word x) h))
      (values (if (hash-has-key? stemming-table word) (hash-ref stemming-table word) word)
              x)))
  (for/list ((doc documents))
    (document (document-kind doc)
              (document-name doc)
              (stem (remove-stopwords (document-hash doc))))))

(define (read-documents options dev)
  (define-values (occupations tasks) (read-occupation-file (hash-ref options 'path-to-occupations-file) dev))
  (define resumes (dir->documents (hash-ref options 'path-to-resumes-dir)
                                      (hash-ref options 'fraction-resume-retained)
                                      'resume
                                      #f)); read all documents
  (define jobs (dir->documents (hash-ref options 'path-to-jobs-dir)
                               1.0
                               'job
                               dev))
  (append occupations tasks resumes jobs))
                  
; MAIN
(define (go)
  (define verbose #f)
  (when #t (print-options options))
  (define dev (hash-ref options 'dev))

  ; read and report on documents
  (define raw-documents (read-documents options dev))
  (define (p kind)
    (define docs-kind (filter (lambda (doc) (equal? kind (document-kind doc))) raw-documents))
    (define docs-kind-vocabulary (documents-hash-union docs-kind))
    (printf "read ~a documents of kind ~a; these contained ~a words, of which ~a were unique~n"
            (length docs-kind)
            kind
            (foldl + 0 (hash-values docs-kind-vocabulary))
            (hash-count docs-kind-vocabulary)))
  (for ((kind (list 'occupation 'resume 'job)))
    (p kind))
  (define raw-vocabulary (documents-hash-union raw-documents))
  (define n-raw-vocabulary-words (foldl + 0 (hash-values raw-vocabulary)))
  (printf "~nthe vocabulary across all kinds of documents has ~a words, of which ~a are unique~n"
          n-raw-vocabulary-words
          (hash-count raw-vocabulary))
  (define (frequency word) (exact->inexact (/ (hash-ref raw-vocabulary word)
                                              n-raw-vocabulary-words)))

  ; stopwords ref: https://snowballstem.org/algorithms/english/stop.txt
  (define stopwords-list
    (list "i" "me" "my" "myself" "we" "our" "ours" "ourselves" "you" "your" "yours" "yourself" "yourselves"
          "he" "him" "his" "himself" "she" "her" "hers" "herself" "it" "its" "itself" "they" "them" "their" "theirs" "themselves"
          "what" "which" "who" "whom" "this" "that" "these" "those"
          "am" "is" "are" "was" "were" "be" "been" "being"
          "have" "has" "had" "having"
          "do" "does" "did" "doing"
          "would" "should" "could" "out"
          "i'm" "you're" "he's" "she's" "it's" "we're" "they're"
          "i've" "you've" "we've" "they've"
          "i'd" "you'd" "he'd" "she'd" "we'd" "they'd" "i'll" "you'll" "he'll" "she'll" "we'll" "they'll"
          "isn't" "aren't" "wasn't" "weren't" "hasn't" "haven't" "hadn't" "doesn't" "don't" "didn't"
          "won't" "wouldn't" "shouldn't" "can't" "cannot" "couldn't" "mustn't"
          "let's" "that's" "who's" "what's" "here's" "there's" "when's" "where's" "why's" "how's"
          "a" "an" "the"
          "and" "but" "if" "or" "because" "as" "until" "while"
          "of" "at" "by" "for" "with" "about" "against" "between" "into" "through" "during" "before" "after" "to" "from" "up" "down"
          "in" "out" "on" "off" "over" "under"
          "again" "further" "then" "once"
          "here" "there" "when" "where" "why" "how"
          "all" "any" "both" "each" "few" "more" "most" "other" "some" "much"
          "no" "nor" "not" "only" "own" "same" "so" "than" "too" "very"))
  (define stopwords
    (for/hash ((stopword stopwords-list))
      (values stopword 1)))
  (printf "~nusing ~a stopwords~n" (hash-count stopwords))
  (when verbose
    (printf " stopwords:")
    (for ((k (sort (hash-keys stopwords) (lambda (a b) (string<? a b)))))
      (printf " ~a" k))
    (printf "~n"))
 
  ; stemming
  (define stemmed-voc (string-split (read-file-at-path (hash-ref options 'path-to-stemmed-voc-file))))
  ;(when verbose (printf "stemmed-voc: ") (printf " ~a" stemmed-voc) (printf "~n"))
  (define stemmed-output (string-split (read-file-at-path (hash-ref options 'path-to-stemmed-output-file))))
  (define stemming-table
    (for/hash ((input stemmed-voc)
               (output stemmed-output))
      (values input output)))
  (printf "~nthe stemming table has ~a entries~n" (hash-count stemming-table))
  (when verbose
    (for ((k (sort (hash-keys stemming-table) (lambda (a b) (string<? a b)))))
      (printf " stem ~a to ~a~n" k (hash-ref stemming-table k))))
        
  (define all-documents-normalized (remove-stopwords-then-stem raw-documents stemming-table stopwords))
 
  ; set weights in each corpus to either counts or tf-idf values; they are currently counts
  (define all-documents (make-weights all-documents-normalized
                                      (hash-ref options 'weights-algorithm)))

  ; match each resume to occupations
  (printf "~nMatching resumes to occupations~n")
  (define (best-n-matches query docs n); -> (values (list docs-name) (list cosine-similarities))
    (define matches (for/hash ((doc docs))
                      (values (document-name doc) (cosine-similarity (document-hash query) (document-hash doc)))))
    (define sorted-keys (head (reverse (hash-keys-sorted-by-value matches))
                              n))
    (define sorted-values (for/list ((sorted-key sorted-keys)) (hash-ref matches sorted-key)))
    (values sorted-keys sorted-values))
  (for ((resume (filter (lambda (doc) (equal? 'resume (document-kind doc))) all-documents)))
    (printf "matches on occupations for resume: ~a~n" (document-name resume))
    (define-values (matched-names matched-scores)
      (best-n-matches resume
                      (filter (lambda (doc) (equal? 'occupation (document-kind doc)))
                              all-documents)
                      (hash-ref options 'n-occupation-matches)))
    (for ((matched-name matched-names)
          (matched-score matched-scores))
      (printf "  resume ~a matches occupation ~a with score ~a~n"
                (document-name resume)
                matched-name
                matched-score)))

  ; match each resume to occupation, then the occupation's tasks to other tasks
  (printf "~nMatching resumes to occupations, then occupations to tasks~n")
  (for ((resume (filter (lambda (doc) (equal? 'resume (document-kind doc))) all-documents)))
    (printf "matches on occupations for resume: ~a~n" (document-name resume))
    (define-values (matched-names matched-scores)
      (best-n-matches resume
                      (filter (lambda (doc) (equal? (document-kind doc) 'occupation))
                              all-documents)
                      (hash-ref options 'n-occupation-matches)))
    (for ((occupation-matched-name matched-names)
          (occupation-matched-score matched-scores))
      (printf "  resume ~a matches occupation ~a with score ~a~n"
              (document-name resume)
              occupation-matched-name
              occupation-matched-score)
      (define query-tasks-doc-list
        (filter (lambda (doc) (and (equal? (document-kind doc) 'task)
                                   (equal? (document-name doc) occupation-matched-name)))
                all-documents))
      ;(printf "query-tasks-doc-list: ~a~n" query-tasks-doc-list)
      (when (not (equal? 1 (length query-tasks-doc-list)))
        (error 'go "found ~a tasks document, not the expected 1" (length query-tasks-doc-list)))
      (define query-tasks-doc (car query-tasks-doc-list))
      (define-values (tasks-matched-names tasks-matched-scores)
        (best-n-matches query-tasks-doc
                        (filter (lambda (doc) (equal? (document-kind doc) 'task))
                                all-documents)
                        (hash-ref options 'n-task-matches)))
      ;(printf "tasks-matched-names: ~a~n" tasks-matched-names)
      (for ((tasks-matched-name tasks-matched-names)
            (tasks-matched-score tasks-matched-scores))
        (printf "    the tasks for the occupation above match the tasks for occupation ~a with score ~a~n"
                tasks-matched-name
                tasks-matched-score))))

  (when (hash-ref options 'dev) (printf "WARNING: dev mode~n"))
                                  
  
  (exit)
              
              
        
  (define occupation-documents (filter-documents 'occupation all-documents))
  (define task-documents (filter-documents 'task all-documents))
  (define resume-documents (filter-documents 'resume all-documents))
  (define job-documents (filter-documents 'job all-documents))
  (define (sorted-matches query others)
    (define matches (for/list ((other others))
                      (cons (document-name other)
                            (cosine-similarity query other))))
    (sort matches (lambda (a b) (< (cdr a) (cdr b)))))
  (for ((resume-document resume-documents))
    (define matching-occupations (head (sorted-matches resume-document occupation-documents)
                                       (hash-ref options 'n-occupation-matches)));-> (listof (document.score))
    (for ((matching-occupation matching-occupations))
      (printf "resume ~a matches occupation ~a with score ~a~n"
              (document-name resume-document)
              (car (document-name matching-occupation))
              (cdr matching-occupation)))
    (for ((matching-occupation matching-occupations))
      (define occupation-name (car matching-occupation))
      (define occupation-task-list (car (filter (lambda (doc) (equal? occupation-name) (document-name doc))
                                                task-documents)))
      (define matching-task-lists (head (sorted-matches occupation-task-list task-documents)
                                        (hash-ref options 'n-task-matches)))
      (for ((matching-task-list matching-task-lists))
        (print "resume ~a tasks match tasks for occupation ~a with score ~a~n"
               (document-name resume-document)
               (car (document-name matching-task-list))
               (cdr matching-task-list)))))

  ; match each resume to all jobs
  (for ((resume-document resume-documents))
    (define matching-jobs (head (sorted-matches resume-document job-documents)
                                (hash-ref options
                                          'n-job-matches)))
    (for ((matching-job matching-jobs))
      (printf "resume ~a matches job ~a with score ~a~n"
              (document-name resume-document)
              (document-name (car matching-job))
              (cdr matching-job)))))



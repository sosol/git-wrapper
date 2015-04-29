(ns git-wrapper.core
  (:require [clojure.java.io :as io] 
            [clojure.java.shell :as shell]
            [clojure.string :as st])
  (:import [java.nio.file Files Paths CopyOption]
           [java.util.zip DeflaterOutputStream]))

(def ^:dynamic sourcerepo nil)
(def ^:dynamic destrepo nil)

(defn substring-after
  "Returns the part of string1 that comes after the first occurrence of string2, or 
  nil if string1 does not contain string2."
  [string1 string2]
  (if (.contains string1 string2) (.substring string1 (+ (.indexOf string1 string2) (.length string2))) nil))

(defn substring-before
   "Returns the part of string1 that comes before the first occurrence of string2, or 
  nil if string1 does not contain string2."
  [string1 string2]
  (if (.contains string1 string2) (.substring string1 0 (.indexOf string1 string2)) nil))

(defn get-name
  [line]
  (substring-after line "\t"))

(defn get-type
  [line]
  (.substring line 7 11))

(defn get-hash
  [line]
  (.substring line 12 52))

(defn get-branch-head
  [name]
  (st/trim-newline (:out (shell/sh "git" (str "--git-dir=" sourcerepo) "rev-parse" name))))

(defn read-tree
  [tree]
  (loop [lines (seq tree) result {(get-name (first lines)) [(get-type (first lines)) (get-hash (first lines))]}]
    (if (seq (rest lines))
      (recur (rest lines) (assoc result (get-name (first lines)) [(get-type (first lines)) (get-hash (first lines))]))
      result)))

(defn load-tree
  [sha]
  (:out (shell/sh "git"  (str "--git-dir=" sourcerepo) "ls-tree" sha)))

(defn has?
  [sha dir]
    (= 0 (:exit (shell/sh "git" (str "--git-dir=" dir) "cat-file" "-e" sha))))

(defn read-commit
  [sha dir]
  (let [commit (:out (shell/sh "git" (str "--git-dir=" dir) "cat-file" "-p" sha))
        c (st/split-lines commit)]
    {:self sha
     :tree (substring-after (first c) " ")
     :parent (if (.startsWith (nth c 2) "parent") 
               [(substring-after (second c) " ") (substring-after (nth c 2) " ")] 
               [(substring-after (second c) " ")])}))

(defn diff-trees
  "Takes two tree sha hashes and outputs a vector of type/hash pairs representing lines where 'new' is different or has additional data from 'base'."
  [newtree basetree]
  (let [t1 (read-tree (st/split-lines (load-tree newtree)))
        t2 (read-tree (st/split-lines (load-tree basetree)))
        diffs (select-keys t1 (filter #(or (not (contains? t2 %)) (not= (second (t2 %)) (second (t1 %)))) (keys t1)))]
    (loop [k (keys diffs) result {(first k) (conj (diffs (first k)) (second (t2 (first k))))}]
      (if (seq (rest k))
        (recur (rest k) (assoc result (first k) (conj (diffs (first k)) (second (t2 (first k))))))
        result))))

(defn accumulate-diffs
  [newsha basesha diffs]
  (if (= "blob" (first (second (last diffs))))
    diffs
    (let [diff (diff-trees newsha basesha)]
       (loop [d diff result diffs]
         (if (seq (rest d))
           (recur (rest d) (conj result (accumulate-diffs (second (second (first d))) (nth (second (first d)) 2) (conj diffs (first d)))))
           (accumulate-diffs (second (second (first d))) (nth (second (first d)) 2) (conj diffs (first d))))))))

(defn get-diffs
  [branchsha sourcesha]
  (let [branch (read-commit branchsha sourcerepo)
        source (read-commit sourcesha sourcerepo)]
    (accumulate-diffs (:tree branch) (:tree source) [[:top ["tree" (:tree branch) (:tree source)]]])))

(defn get-commits
  ([head]
   (if-not (has? head destrepo)
     (get-commits head [(read-commit head sourcerepo)])
     []))
  ([head commits]
   (if-not (has? head destrepo) 
     (let [c (read-commit head sourcerepo)]
       (loop [p (:parent c) result commits]
         (if (seq (rest p)) 
           (if-not (has? (first p) destrepo)
             (recur (rest p) (get-commits (first p) (conj result (read-commit (first p) sourcerepo))))
             (recur (rest p) result))
           (if-not (has? (first p) destrepo)
             (get-commits (first p) (conj result (read-commit (first p) sourcerepo)))
             result))))
     commits)))

(defn object-location
  [sha dir]
  (str dir "/objects/" (.substring sha 0 2) "/" (.substring sha 2)))

(defn loose-object
  "Takes an object SHA has and returns a File representing the object's location. That File may not exist if the object is already packed."
  [sha dir]
  (java.io.File. (object-location sha dir)))

(defn save-commit-object
  [sha]
  (shell/sh "printf" "\"commit" "$(git" (str "--git-dir=" sourcerepo) "cat-file" "-p" sha "|" "wc" "-c" "|" "awk" "'{" "print" "$1" "}')\000\"" ">" (str "/tmp/" sha))
  (shell/sh "git" (str "--git-dir=" sourcerepo) "cat-file" "-p" sha ">>" (str "/tmp/" sha))
  (io/copy (str "/tmp/" sha) (DeflaterOutputStream. (io/output-stream (loose-object sha destrepo)))))

(defn save-object
  [sha type]
  (if (not (has? sha destrepo))
    (if (.exists (loose-object sha sourcerepo)) 
      (Files/copy (.toPath (loose-object sha sourcerepo)) (.toPath (loose-object sha destrepo)) (make-array CopyOption 0))
      (case type
        "blob" (shell/sh "git" (str "--git-dir=" sourcerepo) "cat-file" "blob" sha "|" "git" (str "--git-dir=" destrepo) "hash-object" "-w" "--stdin")
        "tree" (shell/sh "git" (str "--git-dir=" sourcerepo) "ls-tree" sha "|" "git" (str "--git-dir=" destrepo) "mktree" "--missing")
        "commit" (save-commit-object sha)))))

(defn remote-name
  [repo]
  (substring-before (.substring repo (inc (.lastIndexOf repo  "/" (dec (.lastIndexOf repo "/"))))) ".git"))

(defn update-ref
  [sha ref]
  (shell/sh "git" (str "--git-dir=" destrepo) "update-ref" (str "refs/heads/" (substring-after (remote-name sourcerepo) "/") ref) sha))

(defn fetch-lite
  [branch source dest]
  (binding [sourcerepo source destrepo dest] 
    (let [head (get-branch-head branch)
          commits (get-commits head)
          desthead (first (:parent (last commits)))
          objects (loop [c commits objects []] ;;TODO Bug if no commits retrieved
                    (if (seq (rest c))
                      (recur (rest c) (concat (concat objects (get-diffs (:self (first c)) desthead))))
                      (concat (concat objects (get-diffs (:self (first c)) desthead)))))]
      ;;(prn objects)
      ;; write blobs
      (doseq [blob (filter #(= (first (second %)) "blob") objects)]
        (prn (second (second blob)))
        (save-object (second (second blob)) "blob"))
      ;; write trees
      (doseq [tree (filter #(= (first (second %)) "tree") objects)]
        (prn (second (second tree)))
        (save-object (second (second tree)) "tree"))
      ;; copy commits as loose objects
      (doseq [commit commits]
        (prn (:self commit))
        (save-object (:self commit) "commit"))
      (update-ref head branch))))



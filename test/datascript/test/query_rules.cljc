(ns datascript.test.query-rules
  (:require
    [clojure.test :as t :refer [is are deftest testing]]
    [datascript.core :as d]
    [datascript.db :as db]
    [datascript.test.core :as tdc]))

(deftest test-rules
  (let [db [                  [5 :follow 3]
            [1 :follow 2] [2 :follow 3] [3 :follow 4] [4 :follow 6]
            [2         :follow           4]]]
    (is (= (d/q '[:find  ?e1 ?e2
                  :in    $ %
                  :where (follow ?e1 ?e2)]
             db
             '[[(follow ?x ?y)
                [?x :follow ?y]]])
          #{[1 2] [2 3] [3 4] [2 4] [5 3] [4 6]}))
    
    (testing "Joining regular clauses with rule"
      (is (= (d/q '[:find ?y ?x
                    :in $ %
                    :where [_ _ ?x]
                    (rule ?x ?y)
                    [(even? ?x)]]
               db
               '[[(rule ?a ?b)
                  [?a :follow ?b]]])
            #{[3 2] [6 4] [4 2]})))
    
    (testing "Rule context is isolated from outer context"
      (is (= (d/q '[:find ?x
                    :in $ %
                    :where [?e _ _]
                    (rule ?x)]
               db
               '[[(rule ?e)
                  [_ ?e _]]])
            #{[:follow]})))

    (testing "Rule with branches"
      (is (= (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
               db
               1
               '[[(follow ?e2 ?e1)
                  [?e2 :follow ?e1]]
                 [(follow ?e2 ?e1)
                  [?e2 :follow ?t]
                  [?t  :follow ?e1]]])
            #{[2] [3] [4]})))

    (testing "Recursive rules"
      (is (= (d/q '[:find  ?e2
                    :in    $ ?e1 %
                    :where (follow ?e1 ?e2)]
               db
               1
               '[[(follow ?e1 ?e2)
                  [?e1 :follow ?e2]]
                 [(follow ?e1 ?e2)
                  [?e1 :follow ?t]
                  (follow ?t ?e2)]])
            #{[2] [3] [4] [6]}))

      (is (= (d/q '[:find ?e1 ?e2
                    :in $ %
                    :where (follow ?e1 ?e2)]
               [[1 :follow 2] [2 :follow 3]]
               '[[(follow ?e1 ?e2)
                  [?e1 :follow ?e2]]
                 [(follow ?e1 ?e2)
                  (follow ?e2 ?e1)]])
            #{[1 2] [2 3] [2 1] [3 2]}))

      (is (= (d/q '[:find ?e1 ?e2
                    :in $ %
                    :where (follow ?e1 ?e2)]
               [[1 :follow 2] [2 :follow 3] [3 :follow 1]]
               '[[(follow ?e1 ?e2)
                  [?e1 :follow ?e2]]
                 [(follow ?e1 ?e2)
                  (follow ?e2 ?e1)]])
            #{[1 2] [2 3] [3 1] [2 1] [3 2] [1 3]})))

    (testing "Mutually recursive rules"
      (is (= (d/q '[:find  ?e1 ?e2
                    :in    $ %
                    :where (f1 ?e1 ?e2)]
               [[0 :f1 1]
                [1 :f2 2]
                [2 :f1 3]
                [3 :f2 4]
                [4 :f1 5]
                [5 :f2 6]]
               '[[(f1 ?e1 ?e2)
                  [?e1 :f1 ?e2]]
                 [(f1 ?e1 ?e2)
                  [?t :f1 ?e2]
                  (f2 ?e1 ?t)]
                 [(f2 ?e1 ?e2)
                  [?e1 :f2 ?e2]]
                 [(f2 ?e1 ?e2)
                  [?t :f2 ?e2]
                  (f1 ?e1 ?t)]])
            #{[0 1] [0 3] [0 5]
              [1 3] [1 5]
              [2 3] [2 5]
              [3 5]
              [4 5]})))

    (testing "Passing ins to rule"
      (is (= (d/q '[:find ?x ?y
                    :in $ % ?even
                    :where
                    (match ?even ?x ?y)]
               db
               '[[(match ?pred ?e ?e2)
                  [?e :follow ?e2]
                  [(?pred ?e)]
                  [(?pred ?e2)]]]
               even?)
            #{[4 6] [2 4]})))
    
    (testing "Using built-ins inside rule"
      (is (= (d/q '[:find ?x ?y
                    :in $ %
                    :where (match ?x ?y)]
               db
               '[[(match ?e ?e2)
                  [?e :follow ?e2]
                  [(even? ?e)]
                  [(even? ?e2)]]])
            #{[4 6] [2 4]})))
    (testing "Calling rule twice (issue-44)"
      (d/q '[:find ?p
             :in $ % ?fn
             :where (rule ?p ?fn "a")
             (rule ?p ?fn "b")]
        [[1 :attr "a"]]
        '[[(rule ?p ?fn ?x)
           [?p :attr ?x]
           [(?fn ?x)]]]
        (constantly true))))


  (testing "Specifying db to rule"
    (is (= (d/q '[:find ?n
                  :in   $sexes $ages %
                  :where ($sexes male ?n)
                  ($ages adult ?n)]
             [["Ivan" :male] ["Darya" :female] ["Oleg" :male] ["Igor" :male]]
             [["Ivan" 15] ["Oleg" 66] ["Darya" 32]]
             '[[(male ?x)
                [?x :male]]
               [(adult ?y)
                [?y ?a]
                [(>= ?a 18)]]])
          #{["Oleg"]})))

  (testing "Rule name validation issue-319"
    (is (thrown-msg? "Unknown rule 'wat in (wat ?x)"
          (d/q '[:find  ?x
                 :in    $ %
                 :where (wat ?x)]
            [] []))))

  (testing "Rule vars validation"
    (is (thrown-msg? "Cannot parse var, expected symbol starting with ?, got: $e1" ;; issue-300
          (d/q '[:find ?e :in $ % :where [?e]]
            (d/empty-db)
            '[[(rule $e1 ?e2)
               [?e1 :ref ?e2]]])))))

;; issue-218
(deftest test-false-arguments
  (let [db    (d/db-with (d/empty-db) 
                [[:db/add 1 :attr true]
                 [:db/add 2 :attr false]])
        rules '[[(is ?id ?val)
                 [?id :attr ?val]]]]
    (is (= #{[1]}
          (d/q '[:find ?id :in $ %
                 :where (is ?id true)]
            db rules)))
    (is (= #{[2]}
          (d/q '[:find ?id :in $ %
                 :where (is ?id false)]
            db rules)))))


; issue-456
; this used to stall for nearly a minute and/or fail with an OOM exception
; due to propagation of a relation with duplicate tuples during rule solving
(deftest test-rule-performance-on-larger-datasets
  (let [now     (fn []
                  #?(:clj  (/ (System/nanoTime) 1000000.0)
                     :cljs (js/performance.now)))
        inline  (fn [db]
                  (d/q '[:find ?e
                         :where [?e :item/status ?status]
                         [(ground "pending") ?status]]
                    db))
        rule    (fn [db]
                  (d/q '[:find ?e
                         :in $ %
                         :where [?e :item/status ?status]
                         (pending? ?status)]
                    db
                    '[[(pending? ?status)
                       [(ground "pending") ?status]]]))
        measure (fn [f & args]
                  (let [start  (now)
                        result (apply f args)]
                    [(- (now) start) result]))
        db      (-> (d/empty-db)
                  (d/db-with (for [x (range 1 50000)]
                               {:db/id       (- x)
                                :item/id     x
                                :item/status (rand-nth ["started" "pending" "stopped"])})))
        [inline-time inline-result] (measure inline db)
        [rule-time rule-result] (measure rule db)]
    ; (println "inline-time" inline-time "ms, rule-time" rule-time "ms")
    (is (= inline-result rule-result))
    ; show that rule performance continues to be within an order of magnitude of inline performance
    (is (<= 0 rule-time (* 10 inline-time)))))

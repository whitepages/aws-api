(ns cognitect.aws.util-test
  (:require [byte-streams :as byte-streams]
            [clojure.test :refer :all]
            [cognitect.aws.util :as util])
  (:import java.nio.ByteBuffer))

(deftest test-sha-256
  (testing "returns sha for empty string if given nil"
    (is (= (seq (util/sha-256 nil))
           (seq (util/sha-256 ""))
           (seq (util/sha-256 (.getBytes ""))))))
  (testing "accepts string, byte array, or ByteBuffer"
    (is (= (seq (util/sha-256 "hi"))
           (seq (util/sha-256 (.getBytes "hi")))
           (seq (util/sha-256 (ByteBuffer/wrap (.getBytes "hi")))))))
  (testing "does not consume a ByteBuffer"
    (let [bb (ByteBuffer/wrap (.getBytes "hi"))]
      (util/sha-256 bb)
      (is (= "hi" (byte-streams/to-string bb))))))

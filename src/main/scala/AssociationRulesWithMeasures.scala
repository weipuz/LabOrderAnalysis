/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.mllib.fpm

import scala.reflect.ClassTag

import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext.fakeClassTag
import org.apache.spark.mllib.fpm.AssociationRulesWithMeasures.Rule
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.rdd.RDD

/**
 * :: Experimental ::
 *
 * Generates association rules from a [[RDD[FreqItemset[Item]]]. This method only generates
 * association rules which have a single item as the consequent.
 */
@Experimental
class AssociationRulesWithMeasures private[fpm] (
    private var minConfidence: Double,
    private var numTranscation: Long) extends Logging with Serializable {

  /**
   * Constructs a default instance with default parameters {minConfidence = 0.8}.
   */
  def this() = this(0.8,1)

  /**
   * Sets the minimal confidence (default: `0.8`).
   */
  def setMinConfidence(minConfidence: Double): this.type = {
    require(minConfidence >= 0.0 && minConfidence <= 1.0)
    this.minConfidence = minConfidence
    this
  }

  def setNumTranscation(numTranscation: Long): this.type = {
    require(numTranscation >= 0.0 )
    this.numTranscation = numTranscation
    this
  }

  /**
   * Computes the association rules with confidence above [[minConfidence]].
   * @param freqItemsets frequent itemset model obtained from [[FPGrowth]]
   * @return a [[Set[Rule[Item]]] containing the assocation rules.
   */
  def run[Item: ClassTag](freqItemsets: RDD[FreqItemset[Item]]): RDD[Rule[Item]] = {
    // For candidate rule X => Y, generate (X, (Y, freq(X union Y)))
    val candidates = freqItemsets.flatMap { itemset =>
      val items = itemset.items
      items.flatMap { item =>
        items.partition(_ == item) match {
          case (consequent, antecedent) if !antecedent.isEmpty =>
            Some((antecedent.toSeq, (consequent.toSeq, itemset.freq)))
          case _ => None
        }
      }
    }

    // First Join to get (X, ((Y, freq(X union Y)), freq(X)))
    // Second Join to get  (Y, ((X, freq(X union Y), freq(X)), freq(Y)))
    // Then generate rules, and filter by confidence
    
    candidates.join(freqItemsets.map(x => (x.items.toSeq, x.freq)))
      .map { case (antecendent, ((consequent, freqUnion), freqAntecedent)) =>
     (consequent, (antecendent, freqUnion, freqAntecedent))
    }.join(freqItemsets.map(x => (x.items.toSeq, x.freq)))
      .map { case (consequent, ((antecendent, freqUnion, freqAntecedent), freqConsequent)) =>
      new Rule(antecendent.toArray, consequent.toArray, freqUnion, freqAntecedent, freqConsequent,numTranscation)
    }.filter(_.confidence >= minConfidence)
  }

  def run[Item](freqItemsets: JavaRDD[FreqItemset[Item]]): JavaRDD[Rule[Item]] = {
    val tag = fakeClassTag[Item]
    run(freqItemsets.rdd)(tag)
  }
}

object AssociationRulesWithMeasures {

  /**
   * :: Experimental ::
   *
   * An association rule between sets of items.
   * @param antecedent hypotheses of the rule
   * @param consequent conclusion of the rule
   * @tparam Item item type
   */
  @Experimental
  class Rule[Item] private[fpm] (
      val antecedent: Array[Item],
      val consequent: Array[Item],
      freqUnion: Double,
      freqAntecedent: Double,
      freqConsequent: Double,
      numTranscation: Long) extends Serializable {

    def support: Double = freqUnion.toDouble / numTranscation
    def confidence: Double = freqUnion.toDouble / freqAntecedent
    def lift: Double = numTranscation * freqUnion.toDouble / (freqAntecedent * freqConsequent) 
    def conviction: Double = if (confidence == 1) -1 else (1 - support) / (1 - confidence)


    require(antecedent.toSet.intersect(consequent.toSet).isEmpty, {
      val sharedItems = antecedent.toSet.intersect(consequent.toSet)
      s"A valid association rule must have disjoint antecedent and " +
        s"consequent but ${sharedItems} is present in both."
    })
  }
}
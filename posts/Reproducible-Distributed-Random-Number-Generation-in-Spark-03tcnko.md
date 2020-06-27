---
title: Reproducible Distributed Random Number Generation in Spark
date: 2020-06-27T21:40:53.877902Z


---

In this post we will use Spark to generate random numbers in a way that is completely independent of how data is partitioned. That is, given a fixed seed, our Spark program will produce the same result across all hardware and settings. To do this, we introduce a new PRNG and use the TestU01 and PractRand test suites to evaluate its quality.

## The Problem

Reproducible pseudo-random sequences are particularly useful when we want to be able to repeat experiments for different values of non-random parameters. For example, let’s say that we are running an experiment that requires a random sample from a dataset. Then we want to be able to vary the parameters of the experiment for exactly the same random sample while also easily be able to run the experiment for other samples.

Most programming languages come with one or more built-in pseudo-random number generators \(PRNGs\). If you are reading this, you have probably used some several times.

There are many types of PRNGs, and most of them are operated in the same way. You set a start state \(the seed\), and then for each invocation you get a fixed number of bits \(typically 32 or 64 at a time, so we refer to these as numbers\). If you know the seed, you can repeatedly generate the same sequence of bits.

‌This fundamental design of the PRNGs doesn’t lend itself well to be used in distributed computing. If we want to generate a random sequence in parallel, we typically will use the same PRNG with different seeds for each parallel process. The problem with this approach is that when the level of parallelism changes, the resulting sequence will also change.

‌Spark SQL provides the `rand()` function, which suffers from exactly this problem. An instance of the PRNG is created for each partition and seeded by the global seed plus the partition index \(see [eval](https://github.com/apache/spark/blame/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/randomExpressions.scala#L42-L44) and [codegen](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/randomExpressions.scala#L97-L105)\). The partitioning may change if your Spark program is executed on different cluster topologies, in different modes, with different settings, or if the defaults change with new versions of Spark.

Some PRNGs, like [the PCG](https://www.pcg-random.org/) and those proposed [here](https://www.deshawresearch.com/resources_random123.html), support advancing a PRNG to an arbitrary state, which specifically could be used to solve our problem. However, for this work I wanted to avoid using third-party libraries.

To summarize, we would like to generate a sequence of random numbers for rows in a dataset using Spark in such a way that the method is completely agnostic to how the data is partitioned and the underlying stack the Spark job is running on.

## Algorithm

The algorithm basically enumerates rows and then apply a hash function to row indices. The hash values are the pseudo-random numbers.

I doubt that this is a novel idea, but while I was researching the problem, I didn’t find any examples of it being used. So before going into details, I would like to share my intuition behind going with this approach.

‌The family of [linear congruential generators](https://en.wikipedia.org/wiki/Linear_congruential_generator) \(LCGs\) share similarities with textbook multiply-mod-prime hash functions \(you know, $h(x)=ax+b \mod p$ for $a$, $b$, and prime $p$ chosen at random\). Basically, the state of the LCG is hashed to generate the next number, which is then the new state. Multiply-mod-prime will, in expectation, disperse the hash values of an input sequence of numbers in $[0, p-1]$, but due to its linearity, hashing the numbers from $1$ to $n$ will give a sequence of evenly spaced numbers \(mod p\). This will not qualify as a sequence of random numbers. However, widely used hash functions like [Murmur3](https://github.com/aappleby/smhasher) and [xxHash](https://github.com/Cyan4973/xxHash) are known to have good avalanching, i.e., a small change in input yields a big change in the output, and dispersion, although no formal properties have been proved for them.

Combining these observations, we arrive at the algorithm.

### Implementation

Scala code for the Spark algorithm is given below.

```scala
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.XXH64
import org.apache.spark.sql.types.{LongType, StructField, StructType}

def addRNGColumn(df: DataFrame, colName: String, seed: Long = 42L): DataFrame = {
    val rddWithIndex = df.rdd.zipWithIndex()
    df.sparkSession.sqlContext.createDataFrame(
      rddWithIndex.map {
        case (row, index) => Row.fromSeq(row.toSeq :+ XXH64.hashLong(index, seed))
      },
      StructType(df.schema.fields :+ StructField(colName, LongType, false))
    )
  }
```

The function takes a DataFrame and produces a new DataFrame with an additional column containing the random numbers. We use the `zipWithIndex()` function on RDDs to generate indices from $1$ to $n$ for the rows of the DataFrame. Note that calling this function will trigger an action \(see [code](https://github.com/apache/spark/blob/e1ea806b3075d279b5f08a29fe4c1ad6d3c4191a/core/src/main/scala/org/apache/spark/rdd/ZippedWithIndexRDD.scala#L50-L54)\).

For hashing, we use xxHash because it comes in the 64 bit version with Spark, and as we shall see in the following section, performs well for this task.

### A note on performance

`zipWithIndex()` is known to be slow — especially when used with DataFrames, where conversion to RDD and back requires further serialization/deserialization of the rows.

A Spark SQL function for adding consecutive indices does not exist. This is most likely because adding consecutive indices to a distributed dataset inherently requires two passes over the data: One for computing the sizes of the partitions needed to offset local indices, and one for adding the indices. This wouldn’t work well with Spark SQL, the query optimizer, and so forth.

`zipWithIndex()` takes exactly the offset approach described above. The same idea can, with little effort, be implemented based on the Spark SQL function `monotonically_increasing_id()`. This will certainly be faster for DataFrames \(I tried\), but comes with other caveats that I don’t want to address in this post.

## Evaluating the Random Sequence

We claim to have an algorithm that solves the problem, but one important question remains to be answered: Does hashing the numbers from $1$ to $n$ using xxHash actually give a "good" random sequence?

In this section we will discuss how to evaluate PRNGs and run some experiments on our “xxHash the numbers from $1$ to $n$”-PRNG.

First, let’s say that a random sequence of bits is a sequence where each bit takes the the value $0$ or $1$ with equal probability independently of the other bits. There are other definitions of randomness out there, but this one is both practical and to the point.

Notice, however, that if we were given a method for determining if a sequence is random according to the definition, any PRNG would fail this test. For any fixed $t$, a PRNG with a $b$-bit state can only generate $2^b$ distinct sequences of length $t$. For any $t>b$, not all possible sequences can be generated, so a generated sequence can not be a sequence satisfying the definition of being random. Cue Von Neumann reference.

![](https://assets.able.bio/media/u/3fe78a8acf5fda99de95303940a2420c/06POUNDSTONE-superJumbo%20%281%29.jpg)_Obligatory picture of John Von Neumann, famous for, among many things, coming up with the first PRNG and saying: “Any one who considers arithmetical methods of producing random digits is, of course, in a state of sin._“ _I didn’t take the picture, so it is not mine._

### Statistical Testing

Instead of trying to prove that a sequence is random we may determine if a sequence is statistically indistinguishable from a truly random sequence. We do this by computing some test statistic for which we know the distribution \(or an approximation of it\) for a random sequence.

For example, the number of zeroes \(or ones\) in sequence of length $n$ satisfying our definition is normally distributed with mean $\frac 1 2 n$ and variance $\frac 1 4 n$. We may compute the mean for a sequence generated by our PRNG and submit it to a statistical test to determine if it is significantly different from the expected distribution.

Clearly, this test would not help us to reject sequences like for example $0^{n/2}1^{n/2}$, so more tests are needed.

Another example of a test is to compute the [Lempel-Ziv parse](https://en.wikipedia.org/wiki/LZ77_and_LZ78) of the sequence. The Lempel-Ziv parse is integral in many of the compression schemes used today, and the inverse relation between randomness and compressibility is quite obvious \(random data does not compress well — see also [Kolmogorov complexity](https://en.wikipedia.org/wiki/Kolmogorov_complexity)\). It has been empirically determined that the number of phrases in the Lempel-Ziv parse of random sequences follows a normal distribution with mean $n/\log n$ and variance $0.266 n/(\log n)^3$.

Again, some sequences can not be declared non-random based on this test because there are non-random sequences that do not compress well in the Lempel-Ziv scheme.

In fact, we may define infinitely many tests, and any PRNG will fail some.

A finite number of tests with various strengths have been suggested in the literature \(see the References to learn more\) and many of them have been implemented in two software libraries for evaluating PRNGs: [TestU01](http://simul.iro.umontreal.ca/testu01/tu01.html) and [PractRand](http://pracrand.sourceforge.net/). A good PRNG should pass the tests in these.

Next, we will test our PRNG with TestU01 and PractRand.

### TestU01

[This blog post](https://www.pcg-random.org/posts/how-to-test-with-testu01.html) does a good job describing how to download, build, and get started with TestU01.

For xxHash, we use the official 64-bit C implementation available on GitHub.

TestU01 is designed to work with PRNGs generating 32 bits for each invocation. We insist on using the 64 bit version of xxHash, so in the following implementation we break hash values into its upper 32 bits and lower 32 bits. The state of the PRNG consists of the counter and binary value indicating if we should return the upper or lower 32 bits of the value. We increment the counter in every second invocation.

```cpp
#include <stdbool.h>
#include "TestU01.h"
#include "xxhash.h"

static unsigned long y = 0U;
static uint64_t hash;
static bool lower = true;

unsigned int xxhash (void) {
    if (lower) {
        hash = XXH64(&y, 8, 42);
        lower = false;
        return (int)hash & 0xFFFFFFFF;
    } else {
        y += 1;
        lower = true;
        return (int)(hash >> 32) & 0xFFFFFFFF;
    }
}

int main() {
    unif01_Gen* gen = unif01_CreateExternGenBits("xxhash", xxhash);

    bbattery_SmallCrush(gen);
    bbattery_Crush(gen);
    bbattery_BigCrush(gen);

    unif01_DeleteExternGenBits(gen);

    return 0;
}
```

TestU01 consists of three collections of tests: SmallCrush, Crush, and BigCrush. The above program runs all three of them. It took around 12 hours on my laptop. The following shows that all three tests passed without any remarks.

```plain
========= Summary results of SmallCrush =========

 Version:          TestU01 1.2.3
 Generator:        xxhash
 Number of statistics:  15
 Total CPU time:   00:00:05.56

 All tests were passed


========= Summary results of Crush =========

 Version:          TestU01 1.2.3
 Generator:        xxhash
 Number of statistics:  144
 Total CPU time:   00:25:07.50

 All tests were passed
 

========= Summary results of BigCrush =========

 Version:          TestU01 1.2.3
 Generator:        xxhash
 Number of statistics:  160
 Total CPU time:   02:42:26.79

 All tests were passed‌
```

### PractRand

PractRand lets you read the sequence to test from STDIN. The following C program implements our algorithm and writes the hash values to STDOUT.

```cpp
#include <stdio.h>
#include "xxhash.h"

static unsigned long y = 1U;

unsigned long xxhash (void) {
    uint64_t hash = XXH64(&y, 8, 42);
    y += 1;
    return hash;
}

int main() {
    while (1) {
        uint64_t h = xxhash();
        fwrite((void*) &h, sizeof(h), 1, stdout);
    }

    return 0;
}
```

[This blog post](https://www.pcg-random.org/posts/how-to-test-with-practrand.html) describes how to set up PractRand. We run the above program and pipe the output to PractRand.

```shell
./xxhash-out | RNG_test stdin64 -tlmaxonly
```

PractRand will run its tests for sequences of increasing size. I let it run for about 30 hours on my laptop, and ended up testing a 4 TB sequence. That is a sequence of $2^{36}$ 64-bit numbers, which was sufficient for my use cases. The output from the run is seen below.

```plain
RNG_test using PractRand version 0.93
RNG = RNG_stdin64, seed = 0xdf55786d
test set = normal, folding = standard (64 bit)

rng=RNG_stdin64, seed=0xdf55786d
length= 256 megabytes (2^28 bytes), time= 3.5 seconds
  Test Name                         Raw       Processed     Evaluation
  BRank(12):384(1)                  R= +14.7  p~=  1.8e-5   unusual
  ...and 158 test result(s) without anomalies

rng=RNG_stdin64, seed=0xdf55786d
length= 512 megabytes (2^29 bytes), time= 7.5 seconds
  Test Name                         Raw       Processed     Evaluation
  BRank(12):384(1)                  R= +14.7  p~=  1.8e-5   unusual
  ...and 168 test result(s) without anomalies

rng=RNG_stdin64, seed=0xdf55786d
length= 1 gigabyte (2^30 bytes), time= 14.9 seconds
  Test Name                         Raw       Processed     Evaluation
  BRank(12):384(1)                  R= +14.7  p~=  1.8e-5   unusual
  ...and 179 test result(s) without anomalies

rng=RNG_stdin64, seed=0xdf55786d
length= 2 gigabytes (2^31 bytes), time= 29.1 seconds
  no anomalies in 191 test result(s)

rng=RNG_stdin64, seed=0xdf55786d
length= 4 gigabytes (2^32 bytes), time= 56.5 seconds
  no anomalies in 201 test result(s)

rng=RNG_stdin64, seed=0xdf55786d
length= 8 gigabytes (2^33 bytes), time= 113 seconds
  no anomalies in 212 test result(s)

rng=RNG_stdin64, seed=0xdf55786d
length= 16 gigabytes (2^34 bytes), time= 226 seconds
  no anomalies in 223 test result(s)

rng=RNG_stdin64, seed=0xdf55786d
length= 32 gigabytes (2^35 bytes), time= 446 seconds
  no anomalies in 233 test result(s)

rng=RNG_stdin64, seed=0xdf55786d
length= 64 gigabytes (2^36 bytes), time= 894 seconds
  no anomalies in 244 test result(s)

rng=RNG_stdin64, seed=0xdf55786d
length= 128 gigabytes (2^37 bytes), time= 1783 seconds
  no anomalies in 255 test result(s)

rng=RNG_stdin64, seed=0xdf55786d
length= 256 gigabytes (2^38 bytes), time= 3488 seconds
  no anomalies in 265 test result(s)

rng=RNG_stdin64, seed=0xdf55786d
length= 512 gigabytes (2^39 bytes), time= 6948 seconds
  no anomalies in 276 test result(s)

rng=RNG_stdin64, seed=0xdf55786d
length= 1 terabyte (2^40 bytes), time= 13815 seconds
  no anomalies in 287 test result(s)

rng=RNG_stdin64, seed=0xdf55786d
length= 2 terabytes (2^41 bytes), time= 27409 seconds
  Test Name                         Raw       Processed     Evaluation
  BCFN(2+2,13-0,T)                  R=  -7.1  p =1-1.3e-3   unusual
  ...and 296 test result(s) without anomalies

rng=RNG_stdin64, seed=0xdf55786d
length= 4 terabytes (2^42 bytes), time= 56881 seconds
  Test Name                         Raw       Processed     Evaluation
  [Low1/64]BCFN(2+0,13-0,T)         R=  +8.2  p =  6.2e-4   unusual
  ...and 307 test result(s) without anomalies
```

As seen, our PRNG also passes the PractRand tests.

However, for short sequences, PractRand reports that the BRank test results are “unusual”. [The PractRand test documentation](http://pracrand.sourceforge.net/Tests_overview.txt) says that “\[f\]ailures on BRank suggest that in some way the PRNG output, or at least part of it, was extremely linear, producable strictly by xoring bits of previous PRNG output.” It may be the case that for small input values with a lot of zeroes, the avalanching of xxHash isn’t as great as we could hope for.

For long sequences the same is reported for the BCFN tests. The documentations says “BCFN failures where the first parameter listed is low are typically similar in meaning to a failure on a DC6 parameterization \[which is\] typical of small chaotic PRNGs with insufficient mixing / avalanche.” Again, the fact that we are hashing a sequence of numbers increasing by one may reflect in the resulting sequence.

### Summary

Our algorithm passed all tests in TestU01 and PractRand. Some tests are marked as “unusual” by PractRand, which may indicate symptoms of weaknesses in the algorithm.

For comparison, the LCG used in Java/Scala and the [Xorshift PRNG used by Spark](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/random/XORShiftRandom.scala) both fail a non negligible number of tests in both TestU01 \(see the results in [this paper](https://dl.acm.org/doi/pdf/10.1145/1268776.1268777)\) and PractRand \(I ran this myself\).

The decision whether our algorithm is strong enough depends on the use case. For something like random partitioning of data into training and validation subsets for machine learning, I would not hesitate to use the algorithm.

### Visualizing the Noise

The header image used for this post is pixel noise drawn from random bits generated by our algorithm.‌ It serves no purpose other than visualizing randomness. Generally, visualization should not be used instead of statistical testing for determining if a sequence is random. But I wanted a header image for this post.

I generated the image by first writing random bits to a file.

```shell
./xxhash-out > random_binary   
```

This will run until ‌you manually stop it.

The file is read and translated to pixels on a canvas using the following Python script.

```python
from tkinter import *

master = Tk()

canvas_width = 1400
canvas_height = 600
w = Canvas(master, width=canvas_width, height=canvas_height)
w.pack()

with open("random_binary", "rb") as f:
    byte = f.read(1)
    bit = 0

   for x in range(0, canvas_width, 2):
	for y in range(0, canvas_height, 2):
	    if bit > 7:
		bit = 0
		byte = f.read(1)

	    r = (int.from_bytes(byte, 'big') >> bit) & 1
	    bit += 1
	    if r == 1:
		w.create_rectangle((x, y), (x + 2, y + 2), outline="", fill="#616161")
	    else:
	        w.create_rectangle((x, y), (x + 2, y + 2), outline="", fill="#cccccc")

mainloop()
```

## ‌References

* [High Speed Hashing for Integers and Strings](https://arxiv.org/pdf/1504.06804.pdf) \(Mikkel Thorup\). Lectures notes that, among many others, describe the modulo-mod-prime family of hash functions.
* [“What Is a Random Sequence?”](https://www.maa.org/sites/default/files/pdf/upload_library/22/Ford/Volchan46-63.pdf) \(Sergio B. Volcan\). A discussion on the definition of randomness.
* [“A Statistical Test Suite for Random and Pseudorandom Number Generators for Cryptographic Applications”](https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-22r1a.pdf) \(A. Ruhkin et al.\) Technical report with detailed description of statistical tests for evaluating PRNGs.
* [“TestU01: A C Library for Empirical Testing of Random Number Generators”](https://dl.acm.org/doi/pdf/10.1145/1268776.1268777) \(Pierre L’Ecuyer and Richard Simard\). Descriptions of statistical tests used in TestU01 and test results for a number of PRNGs.
* [“PCG: A Family of Simple Fast Space-Efficient Statistically Good Algorithms for Random Number Generation”](https://www.pcg-random.org/pdf/hmc-cs-2014-0905.pdf) \(Melissa E. O’Neill\). PRNGs under the hood. 

‌
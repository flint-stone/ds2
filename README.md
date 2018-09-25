# DS2: fast, accurate, automatic scaling decisions for distributed streaming dataflows.

DS2 is a low-latency controller for dynamic scaling of streaming analytics applications. It can accurately estimate parallelism for all dataflow operators within a _single_ scaling decision, and operates _reactively_ online. DS2 bases scaling decisions on real-time performance traces which it collects throuhg lightweight system-level instrumentation.

This repository contains the following DS2 components:
* The **Scaling policy** implements the scaling model and estimates operator parallelism using metrics collected by the reference system instrumentation.
* The **Scaling manager** periodically invokes the policy when metrics are available and sends scaling commands to the reference stream processor.
* The **Apache Flink 1.4.1 instrumentation patch** contains the necessary instrumentation for Flink to be integrated with DS2.

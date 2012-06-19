.. Aaws documentation master file, created by
   sphinx-quickstart on Sat Jul  2 01:49:54 2011.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to Aaws's documentation!
================================

Contents:

.. toctree::
   :maxdepth: 1

   sqs
   simpledb
   sns
   cloudwatch

Introduction
------------

Aaws is a library for accessing Amazon's Web Services. It currently provides
access to `SQS`, `SNS`, `Cloud Watch`, and `SimpleDB`. Aaws supports both a
traditional synchronous RPC style programming model, and a less intuitive but
for more powerful bulk parallel request programming model.

.. _SQS: http://aws.amazon.com
.. _SNS: http://aws.amazon.com
.. _Cloud Watch: http://aws.amazon.com
.. _SimpleDB: http://aws.amazon.com

Documentation
-------------

* `About Aaws's design <design.html>`_
* `SQS Reference <sqs.html>`_

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`


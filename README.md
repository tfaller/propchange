# propchange
Propchange is/will be a module and service to simply answer the question:

> If property X of document Y changes, what other documents are affected by this?

This question sounds simple, but if you have millions of documents from multiple sources,
its not that easy.

The module/service has a simple interface. It is planed to have multiple backends to perform
this task. Currently only a MySQL backend exists. Also, currently it's just a module, no standalone
service.

## Features
* Simple "Detector" interface
* MySQL backend
* In-Memory backend

## Planned
* More backends
* Standalone service
* SQS integration
* REST and or other Web API
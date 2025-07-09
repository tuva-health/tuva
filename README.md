### The Tuva Project ###
* Need to decide input requirements
  * Should bill type always be 3-digits? Should we allow 4-digits?
  * Should ICD codes always be formatted without the dot?

* Normalize Input
  * If using the DQ checks, this mostly does nothing.

* Conventions
  * Folders under models are used for modular grouping of related models.
  * A folder can represent a module or a grouping of modules.
  * Within a module, there should be three folders: staging, intermediate, final
    * Staging: Models that reference models from other modules. Can include simple joins to reference tables.
    * Intermediate: Models that have business logic that produce intermediate results.
    * Final: Models that produce tables to be used in other modules or for user consumption.
  * Only make external references in the Staging layer. This makes it easier to identify dependencies.
  * Staging and Intermediate models should only be referenced within the module they are defined.
  * Final models can be references by other modules.
  * 
## WIP
This is a project to replace EFCore.BulkExtensions due to the broken functionality regarding the handling of identity values on inserted or updated columns within the database.

This code can currently handle returning the updated auto incremented values of columns, as well as insert child entities. Will also update foreign key id fields based on parent entity.

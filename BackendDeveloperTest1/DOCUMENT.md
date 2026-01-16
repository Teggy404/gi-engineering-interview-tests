# **Recommendations For GI Test1**

The Sections below contain recomendations for table indexing and project structure for the Test1 api project. To maintain consistency with the existing codebase, I did not implement most of the following suggestions (though some were commented in).

## **Table Indexing**

##### *Guid*

All three tables expose a Guid column that is frequently used by my endpoints to identify resources. Adding indexes on the Guid columns would avoid full table scans, resulting in quicker look ups.

##### *Member Accounts*
My member controller frequently uses m.AccountUid to retrieve, count, update, and delete members associated with an account. The addition of the member(AccountUid) index would improve performance for these queries

##### *Primary Members*
Along with the AccountUid index, I would add a unique partial index by AccountId and filter by the primary field. This would index only primary members and enforce the constraints from Task 4 at the database level. 

## **Project Structure**

##### *Dtos*
While the single dto in location controller isnt immediately problematic, it can quickly make the file very cluttered as more endpoints are added. We can fix this by adding a dedicated dtos folder, allowing us to organize our dtos per controller.

##### *Services*
From my experience working with JavaScript frameworks, it is usually best to keep business logic away from controllers. I would add a service layer that would handle query decisions, and leave the controller to returning correct data and calling services.

##### *Querying*
In addition to creating a services section in our project, we can further clean up the structure by storing our SQL queries in their own folder. The services would then reference this query folder to carry out business logic. This would ultimately make the codebase easier to read, debug, and maintain.

##### *SqlBuilder*
I noticed that sql builder was used in every endpoint provided in location controller regardless of the nature of the query. After researching the tool, I learned that this is often used for dynamic querying, so its use in some static queries is unnecessary. By using the static sql directly in the query you could certainly remove alot of extra lines from the codebase. 

##### *Transactions*
It appears that a transaction is being used for endpoints with only single query executions. While it doesnt add too much clutter it is usually unnecessary to perform transactions on such queries, and would allow us to omit the commit/rollback lines where not necessary. One way to approach this would be to use an Isession object instead of dbContext.  

##### *Validation*
It is important that input from the client gets validated to protect our endpoints from bad data. I have added minimal validators to dtos to ensure that data types match, and have kept the checks outside of the controllers. However, it would be better to add standard dotnet validation tools to make sure the shape of the input data is correct.



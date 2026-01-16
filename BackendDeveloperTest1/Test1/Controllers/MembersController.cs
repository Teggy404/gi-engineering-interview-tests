using System.Reflection.Metadata.Ecma335;
using System.Runtime.InteropServices;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Routing.Template;
using SQLitePCL;
using Test1.Contracts;
using Test1.Dtos;

namespace Test1.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class MembersController : ControllerBase
    {
        private readonly ISessionFactory _sessionFactory;
        public MembersController(ISessionFactory sessionFactory)
        {
            _sessionFactory = sessionFactory;
        }

        // GET: api/members
        [HttpGet]
        public async Task<ActionResult<IEnumerable<MemberDto>>> List(CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken)
                .ConfigureAwait(false);

            const string sql = @"
SELECT
    Guid,
    ""Primary"",
    FirstName,
    LastName,
    Address,
    City,
    Cancelled
FROM ""member"";";

            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql);

            var rows = await dbContext.Session.QueryAsync<MemberDto>(template.RawSql, template.Parameters, dbContext.Transaction).ConfigureAwait(false);

            dbContext.Commit();

            return Ok(rows);
        }
        
        // GET: api/members
        [HttpPost]
        public async Task<ActionResult> Create([FromBody] CreateMemberDto model, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken)
                .ConfigureAwait(false);

            //validate
            var error = model.Validate();
            if (error != null)
            {
                dbContext.Rollback();
                return BadRequest(error);
            }

            //ensure account->location exists and fetch keys
            const string verifyAccountSql = @"
SELECT LocationUid, UID
FROM account
WHERE Guid = @Guid;"; //Gui index would speed up lookup

            var accountBuilder = new SqlBuilder();
            var accountTemplate = accountBuilder.AddTemplate(verifyAccountSql, new { Guid = model.AccountGuid });
            var accountKeys = await dbContext.Session.QueryFirstOrDefaultAsync<CreateMemberKeysDto>(accountTemplate.RawSql, accountTemplate.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            //rollback if location does not exist
            if (accountKeys == null)
            {
                dbContext.Rollback();
                return NotFound("Account not found");
            }

            string sql;

            //A unique partial index on account based on primary would remove the need for this check
            if (model.Primary == 1)
            {
                /* This query will attempt to insert a primary user using SELECT and 
                checking for the existence of a primary member under the same account */
                sql = @"
INSERT INTO ""member"" (
    Guid, AccountUid, LocationUid, JoinedDateUtc, CreatedUtc, ""Primary"", 
    FirstName, LastName, Address, City, Locale, PostalCode, Cancelled
) SELECT 
    @Guid, @AccountUid, @LocationUid, @JoinedDateUtc, @CreatedUtc, @Primary, 
    @FirstName, @LastName, @Address, @City, @Locale, @PostalCode, @Cancelled
FROM (SELECT 1)
WHERE NOT EXISTS (
    SELECT 1
    FROM ""member"" m
    WHERE m.AccountUid = @AccountUid AND m.""Primary"" = 1);";
            }
            else
            {
                /* This query will attempt to insert a non-primary member*/
                sql = @"
INSERT INTO ""member"" (
    Guid, AccountUid, LocationUid, JoinedDateUtc, CreatedUtc, ""Primary"", 
    FirstName, LastName, Address, City, Locale, PostalCode, Cancelled
) VALUES (
    @Guid, @AccountUid, @LocationUid, @JoinedDateUtc, @CreatedUtc, @Primary, 
    @FirstName, @LastName, @Address, @City, @Locale, @PostalCode, @Cancelled
);";
            }

            var newGuid = Guid.NewGuid();

            var parameters = new
            {
                Guid = newGuid,
                AccountUid = accountKeys.UID,
                LocationUid = accountKeys.LocationUid,
                CreatedUtc = DateTime.UtcNow,
                Primary = model.Primary,
                JoinedDateUtc = model.JoinedDateUtc ?? DateTime.UtcNow,
                FirstName = model.FirstName,
                LastName = model.LastName,
                Address = model.Address,
                City = model.City,
                Locale = model.Locale,
                PostalCode = model.PostalCode,
                Cancelled = model.Cancelled
            };

            var count = await dbContext.Session.ExecuteAsync(sql, parameters, dbContext.Transaction).ConfigureAwait(false);

            //output appropriate error code
            if (count == 0)
            {
                dbContext.Rollback();
                if (model.Primary == 1) return Conflict("A primary member already exists for this account");
                else return BadRequest("Unable to add member");
            }

            dbContext.Commit();
            //return new member Guid for testing with postman
            return StatusCode(StatusCodes.Status201Created, new { id = newGuid });
        }

        // DELETE: api/members/{Guid}
        [HttpDelete("{id:Guid}")]
        public async Task<ActionResult> DeleteById(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

            //ensure member exists and get keys
            const string GetMemberSql = @"
SELECT UID, AccountUid, ""Primary""
FROM ""member""
WHERE Guid = @Guid;";

            var GetMemberBuilder = new SqlBuilder();
            var GetMembertemplate = GetMemberBuilder.AddTemplate(GetMemberSql, new { Guid = id });
            var memberData = await dbContext.Session.QueryFirstOrDefaultAsync<DeleteMemberInfoDto>(GetMembertemplate.RawSql, GetMembertemplate.Parameters, dbContext.Transaction).ConfigureAwait(false);

            //rollback if member doesnt exist
            if (memberData == null)
            {
                dbContext.Rollback();
                return NotFound();
            }

            //count memebrs in account
            const string CountMembersSql = @"
SELECT COUNT(1)
FROM ""member""
WHERE AccountUid = @AccountUid;";

            var CountMembersBuilder = new SqlBuilder();
            var CountMembertemplate = CountMembersBuilder.AddTemplate(CountMembersSql, new { AccountUid = memberData.AccountUid });
            var MemberCount = await dbContext.Session.QueryFirstOrDefaultAsync<int>(CountMembertemplate.RawSql, CountMembertemplate.Parameters, dbContext.Transaction).ConfigureAwait(false);

            //rollback if only last member
            if (MemberCount == 1)
            {
                dbContext.Rollback();
                return BadRequest("Cannot delete last member");
            }

            //this section could be improved with a unique partial index on accountUid based on the Primary field
            if (memberData.Primary == 1)
            {
                //get next member key
                const string pickMemberSql = @"
SELECT UID
FROM ""member""
WHERE AccountUid = @AccountUid AND UID <> @UID
ORDER BY UID
LIMIT 1;";

                var pickMemberBuilder = new SqlBuilder();
                var pickMemberTemplate = pickMemberBuilder.AddTemplate(pickMemberSql, new { UID = memberData.UID, AccountUid = memberData.AccountUid });
                var newPrimaryUid = await dbContext.Session.QueryFirstOrDefaultAsync<int?>(pickMemberTemplate.RawSql, pickMemberTemplate.Parameters, dbContext.Transaction)
                    .ConfigureAwait(false);

                //rollback if next member doesnt exist
                if (newPrimaryUid == null)
                {
                    dbContext.Rollback();
                    return Conflict("Member to delete not found");
                }

                //promote next member to primary
                const string promoteMemberSql = @"
UPDATE ""member""
SET ""Primary"" = CASE WHEN UID = @UID THEN 1 ELSE 0 END
WHERE AccountUid = @AccountUid;";

                var promoteMemberBuilder = new SqlBuilder();
                var promoteMembertemplate = promoteMemberBuilder.AddTemplate(promoteMemberSql, new { UID = newPrimaryUid.Value, AccountUid = memberData.AccountUid });
                await dbContext.Session.ExecuteAsync(promoteMembertemplate.RawSql, promoteMembertemplate.Parameters, dbContext.Transaction)
                    .ConfigureAwait(false);
            }

            //delete member
            const string DeleteMemberSql = @"
DELETE FROM ""member""
WHERE UID = @UID;";

            var DeleteMemberBuilder = new SqlBuilder();
            var DeleteMemberTemplate = DeleteMemberBuilder.AddTemplate(DeleteMemberSql, new { UID = memberData.UID });
            var count = await dbContext.Session.ExecuteAsync(DeleteMemberTemplate.RawSql, DeleteMemberTemplate.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            //rollback if member was not deleted
            if (count != 1)
            {
                dbContext.Rollback();
                return BadRequest("Unable to delete member");
            }

            dbContext.Commit();
            return Ok();
        }
    }
}
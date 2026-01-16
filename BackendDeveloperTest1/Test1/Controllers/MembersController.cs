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

        [HttpGet]
        public async Task<ActionResult<IEnumerable<MemberDto>>> List(CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

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

        [HttpPost]
        public async Task<ActionResult> Create([FromBody] CreateMemberDto model, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

            var error = model.Validate();
            if (error != null)
            {
                dbContext.Rollback();
                return BadRequest(error);
            }

            const string verifyAccountSql = @"
SELECT LocationUid, UID
FROM account
WHERE Guid = @Guid;";

            var accountBuilder = new SqlBuilder();
            var accountTemplate = accountBuilder.AddTemplate(verifyAccountSql, new { Guid = model.AccountGuid });
            var accountKeys = await dbContext.Session.QueryFirstOrDefaultAsync<CreateMemberKeysDto>(accountTemplate.RawSql, accountTemplate.Parameters, dbContext.Transaction).ConfigureAwait(false);

            if (accountKeys == null)
            {
                dbContext.Rollback();
                return NotFound("Account not found");
            }

            string sql;

            if (model.Primary == 1)
            {
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

            if (count == 0)
            {
                dbContext.Rollback();
                if (model.Primary == 1) return Conflict("A primary member already exists for this account");
                else return BadRequest("Unable to add member");
            }

            dbContext.Commit();

            return StatusCode(StatusCodes.Status201Created, new { id = newGuid });
        }

        [HttpDelete("{id:Guid}")]
        public async Task<ActionResult> DeleteById(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

            const string GetMemberSql = @"
SELECT UID, AccountUid, ""Primary""
FROM ""member""
WHERE Guid = @Guid;";

            var GetMemberBuilder = new SqlBuilder();
            var GetMembertemplate = GetMemberBuilder.AddTemplate(GetMemberSql, new { Guid = id });
            var memberData = await dbContext.Session.QueryFirstOrDefaultAsync<DeleteMemberInfoDto>(GetMembertemplate.RawSql, GetMembertemplate.Parameters, dbContext.Transaction).ConfigureAwait(false);

            if (memberData == null)
            {
                dbContext.Rollback();
                return NotFound();
            }

            const string CountMembersSql = @"
SELECT COUNT(1)
FROM ""member""
WHERE AccountUid = @AccountUid;";

            var CountMembersBuilder = new SqlBuilder();
            var CountMembertemplate = CountMembersBuilder.AddTemplate(CountMembersSql, new { AccountUid = memberData.AccountUid });
            var MemberCount = await dbContext.Session.QueryFirstOrDefaultAsync<int>(CountMembertemplate.RawSql, CountMembertemplate.Parameters, dbContext.Transaction).ConfigureAwait(false);

            if (MemberCount == 1)
            {
                dbContext.Rollback();
                return BadRequest("Cannot delete last member");
            }

            if (memberData.Primary == 1)
            {
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

                if (newPrimaryUid == null)
                {
                    dbContext.Rollback();
                    return Conflict("Member to delete not found");
                }

                const string promoteMemberSql = @"
UPDATE ""member""
SET ""Primary"" = CASE WHEN UID = @UID THEN 1 ELSE 0 END
WHERE AccountUid = @AccountUid;";

                var promoteMemberBuilder = new SqlBuilder();
                var promoteMembertemplate = promoteMemberBuilder.AddTemplate(promoteMemberSql, new { UID = newPrimaryUid.Value, AccountUid = memberData.AccountUid });
                await dbContext.Session.ExecuteAsync(promoteMembertemplate.RawSql, promoteMembertemplate.Parameters, dbContext.Transaction)
                    .ConfigureAwait(false);
            }

            const string DeleteMemberSql = @"
DELETE FROM ""member""
WHERE UID = @UID;";

            var DeleteMemberBuilder = new SqlBuilder();
            var DeleteMemberTemplate = DeleteMemberBuilder.AddTemplate(DeleteMemberSql, new { UID = memberData.UID });
            var count = await dbContext.Session.ExecuteAsync(DeleteMemberTemplate.RawSql, DeleteMemberTemplate.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            if (count != 1)
            {
                dbContext.Rollback();
                return NotFound();
            }

            dbContext.Commit();
            return NoContent();
        }
    }
}
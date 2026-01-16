using System.Data;
using System.Diagnostics;
using System.Xml.Schema;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Routing;
using SQLitePCL;
using Test1.Contracts;
using Test1.Dtos;
using Test1.Models;

namespace Test1.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class AccountsController : ControllerBase
    {
        private readonly ISessionFactory _sessionFactory;

        public AccountsController(ISessionFactory sessionFactory)
        {
            _sessionFactory = sessionFactory;
        }

        // GET: api/accounts
        [HttpGet]
        public async Task<ActionResult<IEnumerable<AccountDto>>> List(CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken)
                .ConfigureAwait(false);
            const string sql = @"
SELECT
    Guid,
    Status,
    AccountType,
    PaymentAmount,
    PendCancel,
    PeriodStartUtc,
    PeriodEndUtc,
    NextBillingUtc
FROM account";

            //SqlBuilder Unnecessary here
            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql);

            var rows = await dbContext.Session.QueryAsync<AccountDto>(template.RawSql, template.Parameters, dbContext.Transaction).ConfigureAwait(false);

            dbContext.Commit();

            return Ok(rows);
        }

        // GET: api/accounts/{Guid}
        [HttpGet("{id:Guid}")]
        public async Task<ActionResult<AccountDto>> GetById(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken)
                .ConfigureAwait(false);

            const string sql = @"
SELECT
    Guid,
    Status,
    AccountType,
    PaymentAmount,
    PendCancel,
    PeriodStartUtc,
    PeriodEndUtc,
    NextBillingUtc
FROM account
/**where**/;";

            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql);

            builder.Where("Guid = @Guid", new
            {
                Guid = id
            });

            var account = await dbContext.Session.QueryFirstOrDefaultAsync<AccountDto>(template.RawSql, template.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            dbContext.Commit();

            return account == null ? NotFound() : Ok(account);
        }

        // GET: api/accounts/{Guid}/members
        [HttpGet("{id:Guid}/members")]
        public async Task<ActionResult<IEnumerable<MemberDto>>> GetMembers(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

            const string sql = @"
SELECT
    m.Guid,
    m.""Primary"",
    m.FirstName,
    m.LastName,
    m.Address,
    m.City,
    m.Cancelled
FROM account a
LEFT JOIN member m ON m.AccountUid = a.UID
/**where**/;
";

            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql);

            builder.Where("a.Guid = @Guid", new
            {
                Guid = id
            });

            var rows = await dbContext.Session.QueryAsync<MemberDto>(template.RawSql, template.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            dbContext.Commit();

            return Ok(rows);

        }
        
        // POST: api/accounts
        [HttpPost]
        public async Task<ActionResult> Create([FromBody] CreateAccountDto model, CancellationToken cancellationToken)
        {
            //transaction makes sense here
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

            //Validate
            var error = model.Validate();
            if (error != null)
            {
                dbContext.Rollback();
                return BadRequest(error);
            }

            //ensure location exists and fetch location Uid
            const string getLocationSql = @"
SELECT UID
FROM location
WHERE Guid = @Guid;
";

            var locationBuilder = new SqlBuilder();
            var getLocationTemplate = locationBuilder.AddTemplate(getLocationSql, new { Guid = model.LocationGuid });
            var location = await dbContext.Session.QueryFirstOrDefaultAsync<int?>(getLocationTemplate.RawSql, getLocationTemplate.Parameters, dbContext.Transaction).ConfigureAwait(false);

            //Rollback if location was not found
            if (location == null)
            {
                dbContext.Rollback();
                return NotFound("Location not found");
            }

            const string sql = @"
INSERT INTO account (
    Guid,
    LocationUid,
    CreatedUtc,
    Status,
    AccountType,
    PaymentAmount,
    PendCancel,
    PeriodStartUtc,
    PeriodEndUtc,
    NextBillingUtc
) VALUES (
    @Guid,
    @LocationUid,
    @CreatedUtc,
    @Status,
    @AccountType,
    @PaymentAmount,
    @PendCancel,
    @PeriodStartUtc,
    @PeriodEndUtc,
    @NextBillingUtc
);";

            var newGuid = Guid.NewGuid();

            var parameters = new
            {
                LocationUid = location.Value,
                Guid = newGuid,
                CreatedUtc = DateTime.UtcNow,
                Status = model.Status,
                AccountType = model.AccountType,
                PaymentAmount = model.PaymentAmount,
                PendCancel = 0,
                PeriodStartUtc = model.PeriodStartUtc,
                PeriodEndUtc = model.PeriodEndUtc,
                NextBillingUtc = model.PeriodStartUtc.AddMonths(1), //one month
            };

            var count = await dbContext.Session.ExecuteAsync(sql, parameters, dbContext.Transaction).ConfigureAwait(false);

            //Rollback if create failed
            if(count != 1)
            {
                dbContext.Rollback();
                return BadRequest("Unable to add account");
            }

            dbContext.Commit();
            //return new account Guid to test with postman
            return StatusCode(StatusCodes.Status201Created, new { id = newGuid });
        }

        // UPDATE: api/accounts/{Guid}
        [HttpPut("{id:Guid}")]
        public async Task<ActionResult> UpdateById(Guid id, [FromBody] UpdateAccountDto model, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

            //Validate Input Data
            var error = model.Validate();
            if (error != null)
            {
                dbContext.Rollback();
                return BadRequest(error);
            }

            //model represents state change, use patch for updating specific fields
            const string sql = @"
UPDATE account
SET
    UpdatedUtc = @UpdatedUtc,
    Status = @Status,
    AccountType = @AccountType,
    PaymentAmount = @PaymentAmount,
    PendCancel = @PendCancel,
    PendCancelDateUtc = @PendCancelDateUtc,
    EndDateUtc = @EndDateUtc
WHERE Guid = @Guid"; //Builder could be used for additional filters

            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql, new
            {
                Guid = id,
                UpdatedUtc = DateTime.UtcNow,
                Status = model.Status,
                AccountType = model.AccountType,
                PaymentAmount = model.PaymentAmount,
                PendCancel = model.PendCancel,
                PendCancelDateUtc = model.PendCancelDateUtc,
                EndDateUtc = model.EndDateUtc,
            });

            var count = await dbContext.Session.ExecuteAsync(template.RawSql, template.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            dbContext.Commit();
            
            if(count == 1)
                return NoContent();
            else
                return NotFound();
        }

        // DELETE: api/accounts/{Guid}
        [HttpDelete("{id:Guid}")]
        public async Task<ActionResult> DeleteById(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken)
                .ConfigureAwait(false);

            //ensure acount exists, get keys
            const string accountKeySql = @"SELECT UID FROM account WHERE Guid = @Guid";

            var accountKeyBuilder = new SqlBuilder();
            var accountKeyTemplate = accountKeyBuilder.AddTemplate(accountKeySql, new { Guid = id });
            var account = await dbContext.Session.QueryFirstOrDefaultAsync<int?>(accountKeyTemplate.RawSql, accountKeyTemplate.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            //rollback if account not found
            if(account == null)
            {
                dbContext.Rollback();
                return NotFound("Account not found");
            }

            //delete members due to forieign key constraint
            const string deleteMemberSql = @"DELETE FROM ""member"" WHERE AccountUid = @AccountUid";

            var deleteMemberBuilder = new SqlBuilder();
            var deleteMemberTemplate = deleteMemberBuilder.AddTemplate(deleteMemberSql, new {AccountUid = account.Value});
            await dbContext.Session.ExecuteAsync(deleteMemberTemplate.RawSql, deleteMemberTemplate.Parameters, dbContext.Transaction);

            //delete account
            const string sql = "DELETE FROM account WHERE Guid = @Guid;";

            var builder = new SqlBuilder();
            var template = builder.AddTemplate(sql, new { Guid = id });
            var count = await dbContext.Session.ExecuteAsync(template.RawSql, template.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);


            //rollback if account not delete
            if (count != 1)
            {
                dbContext.Rollback();
                return BadRequest("Unable to delete account");
            }

            dbContext.Commit();
            return Ok();
        }

        // DELETE: api/accounts/{Guid}/members
        [HttpDelete("{id:Guid}/members")]
        public async Task<ActionResult> DeleteMembersByAccountId(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken)
                .ConfigureAwait(false);

            //Delete non primary members that belong to this account 
            const string sql = @"
DELETE FROM member 
WHERE EXISTS (
    SELECT 1
    FROM account a
    WHERE a.Guid = @Guid 
        AND a.UID = member.AccountUid 
        AND member.""Primary"" = 0
);";// we can use builder for additional filters in the future

            var builder = new SqlBuilder();
            var template = builder.AddTemplate(sql, new
            {
                Guid = id
            });

            var count = await dbContext.Session.ExecuteAsync(template.RawSql, template.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            dbContext.Commit();
            return Ok();
        }
    }
}
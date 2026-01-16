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

        [HttpGet]
        public async Task<ActionResult<IEnumerable<AccountDto>>> List(CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);
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

            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql);

            var rows = await dbContext.Session.QueryAsync<AccountDto>(template.RawSql, template.Parameters, dbContext.Transaction).ConfigureAwait(false);

            dbContext.Commit();

            return Ok(rows);
        }

        [HttpGet("{id:Guid}")]
        public async Task<ActionResult<AccountDto>> GetById(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

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

            var account = await dbContext.Session.QueryFirstOrDefaultAsync<AccountDto>(template.RawSql, template.Parameters, dbContext.Transaction).ConfigureAwait(false);

            dbContext.Commit();

            return account == null ? NotFound() : Ok(account);
        }

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
WHERE a.Guid = @Guid
";

            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql, new
            {
                Guid = id
            });

            var rows = await dbContext.Session.QueryAsync<MemberDto>(template.RawSql, template.Parameters, dbContext.Transaction).ConfigureAwait(false);

            dbContext.Commit();

            return Ok(rows);

        }

        [HttpPost]
        public async Task<ActionResult> Create([FromBody] CreateAccountDto model, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

            //Validate
            var error = model.Validate();
            if (error != null)
            {
                dbContext.Rollback();
                return BadRequest(error);
            }

            const string getLocationSql = @"
SELECT UID
FROM location
WHERE Guid = @Guid;
";

            var locationBuilder = new SqlBuilder();
            var getLocationTemplate = locationBuilder.AddTemplate(getLocationSql, new { Guid = model.LocationGuid });
            var location = await dbContext.Session.QueryFirstOrDefaultAsync<int?>(getLocationTemplate.RawSql, getLocationTemplate.Parameters, dbContext.Transaction).ConfigureAwait(false);

            if (location == null)
            {
                dbContext.Rollback();
                return NotFound("Location not found.");
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
                Status = AccountStatusType.GREEN,
                AccountType = model.AccountType,
                PaymentAmount = model.PaymentAmount,
                PendCancel = 0,
                PeriodStartUtc = model.PeriodStartUtc,
                PeriodEndUtc = model.PeriodEndUtc,
                NextBillingUtc = model.PeriodStartUtc.AddMonths(1), //one month
            };

            var count = await dbContext.Session.ExecuteAsync(sql, parameters, dbContext.Transaction).ConfigureAwait(false);

            dbContext.Commit();

            return count == 1 ? StatusCode(StatusCodes.Status201Created, new { id = newGuid }) : BadRequest("Unable to add account");
        }

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
WHERE Guid = @Guid";

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

            var count = await dbContext.Session.ExecuteAsync(template.RawSql, template.Parameters, dbContext.Transaction).ConfigureAwait(false);

            dbContext.Commit();

            return count == 1 ? NoContent() : NotFound();
        }

        [HttpDelete("{id:Guid}")]
        public async Task<ActionResult> DeleteById(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

            const string sql = "DELETE FROM account WHERE Guid = @Guid;";

            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql, new
            {
                Guid = id
            });

            var count = await dbContext.Session.ExecuteAsync(template.RawSql, template.Parameters, dbContext.Transaction).ConfigureAwait(false);

            dbContext.Commit();

            if (count == 1)
                return Ok();
            else
                return BadRequest("Unable to delete location");

        }

        [HttpDelete("{id:Guid}/members")]
        public async Task<ActionResult> DeleteMembersByAccountId(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken).ConfigureAwait(false);

            const string sql = @"
DELETE FROM member 
WHERE EXISTS (
    SELECT 1
    FROM account a
    WHERE a.Guid = @Guid 
        AND a.UID = member.AccountUid 
        AND member.""Primary"" = 0
);";

            var builder = new SqlBuilder();
            var template = builder.AddTemplate(sql, new
            {
                Guid = id
            });

            var count = await dbContext.Session.ExecuteAsync(template.RawSql, template.Parameters, dbContext.Transaction).ConfigureAwait(false);

            dbContext.Commit();
            return NoContent();
        }
    }
}
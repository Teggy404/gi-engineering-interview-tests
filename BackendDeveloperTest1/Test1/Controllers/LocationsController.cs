using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Linq;
using Dapper;
using Test1.Contracts;
using Test1.Core;
using Test1.Models;
using Test1.Dtos;
using System.Transactions;
using SQLitePCL;


namespace Test1.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class LocationsController : ControllerBase
    {
        private readonly ISessionFactory _sessionFactory;

        public LocationsController(ISessionFactory sessionFactory)
        {
            _sessionFactory = sessionFactory;
        }

        // GET: api/locations
        [HttpGet]
        public async Task<ActionResult<IEnumerable<LocationDto>>> List(CancellationToken cancellationToken)
        {   
            //Recommendation: Instead of creating a dbContext, we can create an Isession using .CreateSessionAsync to avoid unecessary transactions
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken)
                .ConfigureAwait(false);

            const string sql = @"
SELECT
    l.Guid,
    l.Name,
    l.Address,
    l.City,
    l.Locale,
    l.PostalCode,
    COUNT(
        CASE 
            WHEN a.Status IS NOT NULL AND a.Status < @Inactive 
            THEN 1 
        END
        ) AS ActiveCount
FROM location l
LEFT JOIN account a ON a.LocationUid = l.UID
GROUP BY
    l.UID,
    l.Guid,
    l.Name,
    l.Address,
    l.City,
    l.Locale,
    l.PostalCode
;";

            //Recommendation: SqlBuilder Unnecessary here
            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql, new
            {
                Inactive = AccountStatusType.CANCELLED
            });

            var rows = await dbContext.Session.QueryAsync<LocationWithActiveCountDto>(template.RawSql, template.Parameters, dbContext.Transaction
            ).ConfigureAwait(false);

            dbContext.Commit();

            return Ok(rows); // Returns an HTTP 200 OK status with the data
        }

        // GET: api/locations/{Guid}
        [HttpGet("{id:Guid}")]
        public async Task<ActionResult<LocationDto>> GetById(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken)
                .ConfigureAwait(false);

            const string sql = @"
SELECT
    Guid,
    Name,
    Address,
    City,
    Locale,
    PostalCode
FROM location
/**where**/;";

            //Recommendation: Sql builder makes sense here for additional filtering in the future
            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql);

            builder.Where("Guid = @Guid", new
            {
                Guid = id
            });

            //Recommendation: Query FirstOrDefault for single resource
            var rows = await dbContext.Session.QueryAsync<LocationDto>(template.RawSql, template.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            dbContext.Commit();

            //Recommendation: Return error code if resource not found
            return Ok(rows.FirstOrDefault()); // Returns an HTTP 200 OK status with the data
        }

        // POST: api/locations
        [HttpPost]
        public async Task<ActionResult<string>> Create([FromBody] LocationDto model, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken)
                .ConfigureAwait(false);

            const string sql = @"
INSERT INTO location (
    Guid,
    CreatedUtc,
    Disabled,
    EnableBilling,
    AccountStatus,
    Name,
    Address,
    City,
    Locale,
    PostalCode
) VALUES (
    @Guid,
    @CreatedUtc,
    @Disabled,
    @EnableBilling,
    @AccountStatus,
    @Name,
    @Address,
    @City,
    @Locale,
    @PostalCode
);";

            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql, new
            {
                Guid = Guid.NewGuid(),
                CreatedUtc = DateTime.UtcNow,
                Disabled = false,
                EnableBilling = false,
                AccountStatus = AccountStatusType.GREEN,
                model.Name,
                model.Address,
                model.City,
                model.Locale,
                model.PostalCode
            });

            var count = await dbContext.Session.ExecuteAsync(template.RawSql, template.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            dbContext.Commit();

            if (count == 1)
                return Ok();
            else
                return BadRequest("Unable to add location");
        }

        // DELETE: api/locations/{Guid}
        [HttpDelete("{id:Guid}")]
        public async Task<ActionResult<LocationDto>> DeleteById(Guid id, CancellationToken cancellationToken)
        {
            await using var dbContext = await _sessionFactory.CreateContextAsync(cancellationToken)
                .ConfigureAwait(false);

            const string sql = "DELETE FROM location WHERE Guid = @Guid;";

            var builder = new SqlBuilder();

            var template = builder.AddTemplate(sql, new
            {
                Guid = id
            });

            var count = await dbContext.Session.ExecuteAsync(template.RawSql, template.Parameters, dbContext.Transaction)
                .ConfigureAwait(false);

            dbContext.Commit();

            if (count == 1)
                return Ok();
            else
                return BadRequest("Unable to delete location");
        }

        public class LocationDto
        {
            public Guid Guid { get; set; }
            public string Name { get; set; }
            public string Address { get; set; }
            public string City { get; set; }
            public string Locale { get; set; }
            public string PostalCode { get; set; }
        }
    }
}

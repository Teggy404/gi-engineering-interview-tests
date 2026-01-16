#nullable enable

using System.ComponentModel.DataAnnotations;
using Test1.Models;

namespace Test1.Dtos
{
    public class AccountDto
    {
        public Guid Guid { get; set; }
        public AccountStatusType Status { get; set; }
        public AccountType AccountType { get; set; }
        public double? PaymentAmount { get; set; }
        public int PendCancel { get; set; }
        public DateTime PeriodStartUtc { get; set; }
        public DateTime PeriodEndUtc { get; set; }
        public DateTime NextBillingUtc { get; set; }
    }

    public class CreateAccountDto
    {
        public Guid LocationGuid { get; set; }
        public AccountStatusType Status { get; set; }
        public AccountType AccountType { get; set; }
        public double? PaymentAmount { get; set; }
        public DateTime PeriodStartUtc { get; set; }
        public DateTime PeriodEndUtc { get; set; }

        public string? Validate()
        {
            if (LocationGuid == Guid.Empty)
                return "LocationGuid is required.";

            if (!Enum.IsDefined(typeof(AccountType), AccountType))
                return "Invalid AccountType.";

            if (PaymentAmount.HasValue && PaymentAmount.Value < 0)
                return "PaymentAmount cannot be negative.";

            if (PeriodStartUtc == default || PeriodEndUtc == default)
                return "Period dates are required.";

            if (PeriodEndUtc <= PeriodStartUtc)
                return "PeriodEndUtc must be after PeriodStartUtc.";

            return null;
        }
    }

    public class UpdateAccountDto
    {
        public AccountStatusType Status { get; set; }
        public AccountType AccountType { get; set; }
        public double? PaymentAmount { get; set; }
        public int PendCancel { get; set; }
        public DateTime? PendCancelDateUtc { get; set; }
        public DateTime? EndDateUtc { get; set; }

        public string? Validate()
        {
            if (!Enum.IsDefined(typeof(AccountStatusType), Status))
                return "Invalid Status.";

            if (!Enum.IsDefined(typeof(AccountType), AccountType))
                return "Invalid AccountType.";

            if (PendCancel != 0 && PendCancel != 1)
                return "PendCancel must be 0 or 1.";

            if (PaymentAmount.HasValue && PaymentAmount.Value < 0)
                return "PaymentAmount cannot be negative.";

            return null;
        }
    }
}
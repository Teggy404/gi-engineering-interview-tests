#nullable enable

using System.Xml;

namespace Test1.Dtos
{
    public class MemberDto
    {
        public Guid Guid { get; set; }
        public int Primary { get; set; }
        public string? FirstName { get; set; }
        public string? LastName { get; set; }
        public string? Address { get; set; }
        public string? City { get; set; }
        public int Cancelled { get; set; }
    }

    public class CreateMemberDto
    {
        public Guid AccountGuid { get; set; }
        public DateTime? JoinedDateUtc { get; set; }
        public int Primary { get; set; }
        public string? FirstName { get; set; }
        public string? LastName { get; set; }
        public string? Address { get; set; }
        public string? City { get; set; }
        public string? Locale { get; set; }
        public string? PostalCode { get; set; }
        public int Cancelled { get; set; }

        public string? Validate()
        {
            if (AccountGuid == Guid.Empty)
                return "AccountGuid is required.";

            if (Primary != 0 && Primary != 1)
                return "Primary must be 0 or 1.";

            if (Cancelled != 0 && Cancelled != 1)
                return "Cancelled must be 0 or 1.";

            if (LastName != null && string.IsNullOrWhiteSpace(LastName))
                return "LastName cannot be empty.";

            if (PostalCode != null && string.IsNullOrWhiteSpace(PostalCode))
                return "PostalCode cannot be empty.";

            return null;
        }
    }

    public class CreateMemberKeysDto
    {
        public int UID { get; set; }
        public int LocationUid { get; set; }
    }

    public class DeleteMemberInfoDto
    {
        public int UID { get; set; }
        public int AccountUid { get; set; }
        public int Primary { get; set; }
    }
}
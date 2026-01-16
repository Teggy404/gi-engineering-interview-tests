namespace Test1.Dtos
{
    public class LocationWithActiveCountDto
    {
        public Guid Guid { get; set; }
        public string Name { get; set; }
        public string Address { get; set; }
        public string City { get; set; }
        public string Locale { get; set; }
        public string PostalCode { get; set; }
        public int ActiveCount { get; set; }
    }
}
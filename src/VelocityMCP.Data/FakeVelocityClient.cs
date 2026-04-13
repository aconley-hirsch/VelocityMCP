using VelocityMCP.Data.Models;

namespace VelocityMCP.Data;

/// <summary>
/// Generates synthetic Velocity data for Mac development and testing.
/// Produces realistic-looking transactions so the full pipeline can be exercised
/// without a Windows SDK or a live Velocity instance.
/// </summary>
public sealed class FakeVelocityClient : IVelocityClient
{
    private static readonly string[] ReaderNames =
    [
        "Front Door Reader 1", "Front Door Reader 2",
        "Side Office Door", "Loading Dock",
        "Server Room", "Executive Suite",
        "Parking Garage Entry", "Parking Garage Exit",
        "Lobby Turnstile A", "Lobby Turnstile B"
    ];

    /// <summary>
    /// Models the door → readers relationship that Velocity actually has:
    /// some doors (Front Door, Lobby, Parking Garage) have multiple readers (entry + exit),
    /// others have just one.
    /// </summary>
    private static readonly (int DoorId, string DoorName, int[] ReaderIndices)[] DoorDefinitions =
    [
        (1, "Front Door",      [0, 1]),  // Reader 1 + Reader 2
        (2, "Side Office Door",[2]),
        (3, "Loading Dock",    [3]),
        (4, "Server Room",     [4]),
        (5, "Executive Suite", [5]),
        (6, "Parking Garage",  [6, 7]),  // Entry + Exit
        (7, "Lobby",           [8, 9]),  // Turnstile A + B
    ];

    private static readonly string[] FirstNames =
        ["Jane", "John", "Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Hank"];

    private static readonly string[] LastNames =
        ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Martinez", "Wilson"];

    private static readonly (int Code, string Description, int Disposition)[] EventTypes =
    [
        (1, "Access Granted", 1),
        (2, "Access Denied - Invalid Credential", 2),
        (3, "Access Denied - Invalid PIN", 3),
        (4, "Door Forced Open", 0),
        (5, "Door Held Open", 0),
        (6, "Request to Exit", 1),
        (7, "Door Locked", 0),
        (8, "Door Unlocked", 0),
    ];

    private readonly Random _rng = new(42); // deterministic seed for reproducibility
    private int _nextLogId = 1;
    private int _nextAlarmId = 1;
    private bool _connected;

    public bool IsConnected => _connected;

    public Task ConnectAsync(CancellationToken ct = default)
    {
        _connected = true;
        return Task.CompletedTask;
    }

    public Task<List<TransactionRecord>> GetLogTransactionsAsync(DateTime sinceDate, CancellationToken ct = default)
    {
        var records = new List<TransactionRecord>();
        var now = DateTime.UtcNow;

        // Generate ~50 transactions per call, spread between sinceDate and now
        int count = _rng.Next(30, 70);
        for (int i = 0; i < count; i++)
        {
            var ticksRange = now.Ticks - sinceDate.Ticks;
            if (ticksRange <= 0) break;
            var dt = sinceDate.AddTicks(_rng.NextInt64(0, ticksRange));
            var evt = EventTypes[_rng.Next(EventTypes.Length)];
            var personIdx = _rng.Next(FirstNames.Length);
            var readerIdx = _rng.Next(ReaderNames.Length);

            records.Add(new TransactionRecord
            {
                LogId = _nextLogId++,
                DtDate = dt,
                PcDateTime = dt.AddMilliseconds(_rng.Next(-50, 50)),
                EventCode = evt.Code,
                Description = evt.Description,
                Disposition = evt.Disposition,
                TransactionType = 0,
                ReportAsAlarm = evt.Code is 4 or 5,
                AlarmLevelPriority = evt.Code is 4 or 5 ? 3 : 0,
                PortAddr = "1",
                DtAddr = "1",
                XAddr = "0",
                NetAddress = $"192.168.1.{10 + readerIdx}",
                DoorOrExpansion = 0,
                Reader = (byte)(readerIdx + 1),
                ReaderName = ReaderNames[readerIdx],
                FromZone = 0,
                ToZone = 1,
                Uid1 = 1000 + personIdx,
                Uid1Name = $"{FirstNames[personIdx]} {LastNames[personIdx]}",
                Uid2 = 0,
                Uid2Name = null,
                ServerID = 1,
                SecurityDomainID = 1
            });
        }

        return Task.FromResult(records);
    }

    public Task<List<AlarmRecord>> GetAlarmLogAsync(DateTime sinceDate, CancellationToken ct = default)
    {
        var records = new List<AlarmRecord>();
        var now = DateTime.UtcNow;

        int count = _rng.Next(5, 15);
        for (int i = 0; i < count; i++)
        {
            var ticksRange = now.Ticks - sinceDate.Ticks;
            if (ticksRange <= 0) break;
            var dt = sinceDate.AddTicks(_rng.NextInt64(0, ticksRange));
            bool acked = _rng.Next(2) == 0;
            bool cleared = acked && _rng.Next(2) == 0;
            var personIdx = _rng.Next(FirstNames.Length);

            records.Add(new AlarmRecord
            {
                AlarmId = _nextAlarmId++,
                DtDate = dt,
                DbDate = dt.AddMilliseconds(_rng.Next(0, 100)),
                AkDate = acked ? dt.AddMinutes(_rng.Next(1, 60)) : null,
                ClDate = cleared ? dt.AddMinutes(_rng.Next(60, 240)) : null,
                EventId = _rng.Next(2) == 0 ? 4 : 5, // forced or held
                AlarmLevelPriority = _rng.Next(1, 5),
                Status = (byte)(cleared ? 2 : acked ? 1 : 0),
                Description = _rng.Next(2) == 0 ? "Door Forced Open" : "Door Held Open",
                PortAddr = 1,
                DtAddr = 1,
                XAddr = 0,
                NetAddress = $"192.168.1.{10 + _rng.Next(ReaderNames.Length)}",
                AkOperator = acked ? "admin" : null,
                ClOperator = cleared ? "admin" : null,
                WorkstationName = "VELOCITY-WS1",
                Uid1 = 1000 + personIdx,
                Uid1Name = $"{FirstNames[personIdx]} {LastNames[personIdx]}",
                Uid2 = null,
                Uid2Name = null,
                Parm1 = null,
                Parm2 = null,
                TransactionType = 0,
                ServerID = 1,
                SiteID = 1
            });
        }

        return Task.FromResult(records);
    }

    public Task<List<DoorRecord>> GetDoorsAsync(CancellationToken ct = default)
    {
        var doors = DoorDefinitions.Select(d => new DoorRecord
        {
            Id = d.DoorId,
            Name = d.DoorName,
            Index = (byte)d.DoorId,
            ControllerId = 1,
            Address = $"1.1.{d.DoorId}"
        }).ToList();

        return Task.FromResult(doors);
    }

    public Task<List<ReaderRecord>> GetReadersAsync(CancellationToken ct = default)
    {
        // Build a reader→door lookup from DoorDefinitions so each reader knows its door.
        var readerToDoor = DoorDefinitions
            .SelectMany(d => d.ReaderIndices.Select(ri => (ReaderIndex: ri, DoorId: d.DoorId)))
            .ToDictionary(x => x.ReaderIndex, x => x.DoorId);

        var readers = ReaderNames.Select((name, i) => new ReaderRecord
        {
            Id = i + 1,
            Name = name,
            FromZone = 0,
            ToZone = 1,
            DoorId = readerToDoor.TryGetValue(i, out var did) ? did : null
        }).ToList();

        return Task.FromResult(readers);
    }

    public Task<List<PersonRecord>> GetPersonsAsync(CancellationToken ct = default)
    {
        var people = FirstNames.Zip(LastNames).Select((pair, i) => new PersonRecord
        {
            PersonId = 1000 + i,
            FirstName = pair.First,
            LastName = pair.Second
        }).ToList();

        return Task.FromResult(people);
    }

    public void Dispose() { _connected = false; }
}

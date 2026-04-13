using System.ComponentModel;
using System.Text.Json;
using ModelContextProtocol.Server;
using VelocityMCP.Data;

namespace VelocityMCP.Tools;

[McpServerToolType]
public sealed class GetEventTool
{
    [McpServerTool(Name = "get_event", Destructive = false, ReadOnly = true),
     Description("Fetch the full record for a single transaction by its log_id. " +
                 "Use this to drill into a specific event surfaced by sample_events — " +
                 "for example, after 'show me the most recent forced-open events', " +
                 "call get_event on one log_id to see full context (zones, addresses, pc_date_time, priority). " +
                 "Returns null if the log_id is not in the mirror. " +
                 "PII fields (pin/code) are never present — excluded at ingest.")]
    public static string GetEvent(
        DuckDbMirror mirror,
        [Description("The log_id of the transaction to fetch. Look up from sample_events output.")]
        int log_id)
    {
        var detail = mirror.GetTransaction(log_id);
        if (detail == null)
        {
            return JsonSerializer.Serialize(
                new { found = false, log_id },
                new JsonSerializerOptions { WriteIndented = true });
        }

        var payload = new
        {
            found = true,
            log_id = detail.LogId,
            time = detail.DtDate.ToString("o"),
            pc_time = detail.PcDateTime?.ToString("o"),
            event_code = detail.EventCode,
            description = detail.Description,
            disposition = detail.Disposition,
            transaction_type = detail.TransactionType,
            report_as_alarm = detail.ReportAsAlarm,
            alarm_level_priority = detail.AlarmLevelPriority,
            reader_name = detail.ReaderName,
            from_zone = detail.FromZone,
            to_zone = detail.ToZone,
            port_addr = detail.PortAddr,
            dt_addr = detail.DtAddr,
            x_addr = detail.XAddr,
            net_address = detail.NetAddress,
            person_id = detail.PersonId,
            person_name = detail.PersonName,
            uid2 = detail.Uid2,
            uid2_name = detail.Uid2Name
        };

        return JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
    }
}

[McpServerToolType]
public sealed class GetAlarmTool
{
    [McpServerTool(Name = "get_alarm", Destructive = false, ReadOnly = true),
     Description("Fetch the full record for a single alarm by its alarm_id. " +
                 "Use this to drill into a specific alarm surfaced by sample_alarms — returns acknowledgment time, " +
                 "clear time, ak_operator, cl_operator, workstation, priority, and parm1/parm2 payload fields. " +
                 "Returns found=false if the alarm_id is not in the mirror.")]
    public static string GetAlarm(
        DuckDbMirror mirror,
        [Description("The alarm_id of the alarm to fetch. Look up from sample_alarms output.")]
        int alarm_id)
    {
        var detail = mirror.GetAlarm(alarm_id);
        if (detail == null)
        {
            return JsonSerializer.Serialize(
                new { found = false, alarm_id },
                new JsonSerializerOptions { WriteIndented = true });
        }

        var payload = new
        {
            found = true,
            alarm_id = detail.AlarmId,
            time = detail.DtDate?.ToString("o"),
            db_time = detail.DbDate?.ToString("o"),
            ak_time = detail.AkDate?.ToString("o"),
            cl_time = detail.ClDate?.ToString("o"),
            event_id = detail.EventId,
            alarm_level_priority = detail.AlarmLevelPriority,
            status = detail.Status,
            description = detail.Description,
            ak_operator = detail.AkOperator,
            cl_operator = detail.ClOperator,
            workstation_name = detail.WorkstationName,
            net_address = detail.NetAddress,
            person_id = detail.PersonId,
            person_name = detail.PersonName,
            parm1 = detail.Parm1,
            parm2 = detail.Parm2
        };

        return JsonSerializer.Serialize(payload, new JsonSerializerOptions { WriteIndented = true });
    }
}

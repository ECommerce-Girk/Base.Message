using System.Security.Cryptography;
using System.Text.Json;
using Base.Database;
using Microsoft.EntityFrameworkCore;
using Npgsql;

namespace Base.Message;

public sealed class InvalidMessageException(string message) : Exception(message);

public static class MessageHandler
{
    public static async Task Handle(
    DbContextBase db,
    Guid messageId,
    IDomainEvent message,
    object handler,
    CancellationToken ct)
    {
        await db.BeginTransaction(ct);

        try
        {
            var isNewMessage = await AddOrVerifyInboxRecordAsync(db, message, messageId, ct);

            if (isNewMessage)
                await ((dynamic)handler).Handle((dynamic)message, ct);

            await db.CommitTransaction(ct);
        }
        catch
        {
            await db.RollbackTransaction(CancellationToken.None);
            throw;
        }
    }

    private static async Task<bool> AddOrVerifyInboxRecordAsync(
        DbContextBase dbContext,
        IDomainEvent message,
        Guid messageId,
        CancellationToken ct)
    {
        var hash = Convert.ToHexString(SHA256.HashData(JsonSerializer.SerializeToUtf8Bytes(message, message.GetType())));

        var inboxMessage = new InboxMessage
        {
            Id = messageId,
            PayloadHash = hash
        };

        try
        {
            dbContext.InboxMessages.Add(inboxMessage);

            await dbContext.SaveChangesAsync(ct);

            return true;
        }
        catch (DbUpdateException exception)
            when (exception.InnerException is PostgresException postgresException
                && postgresException.SqlState == PostgresErrorCodes.UniqueViolation
                && postgresException.ConstraintName == "PK_InboxMessages")
        {
            dbContext.Entry(inboxMessage).State = EntityState.Detached;

            var existing = await dbContext.InboxMessages
                .AsNoTracking()
                .SingleOrDefaultAsync(x => x.Id == messageId, ct);

            if (existing is null)
                throw;

            if (existing.PayloadHash != hash)
                throw new InvalidMessageException(
                    "Event ID was reused with different content.");

            return false;
        }
    }
}
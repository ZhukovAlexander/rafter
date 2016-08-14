from schematics import models, types
import msgpack


class MsgpackModel(models.Model):

    def pack(self):
        return msgpack.packb(self.to_primitive())

    @classmethod
    def unpack(cls, bytes):
        return cls({k.decode(): value for k, value in msgpack.unpackb(bytes).items()})


class LogEntry(MsgpackModel):
    index = types.IntType()
    term = types.IntType()
    command = types.StringType()


class AppendEntriesRPCRequest(MsgpackModel):
    term = types.IntType()
    leader_id = types.StringType()
    prev_log_index = types.IntType()
    prev_log_term = types.IntType()
    leader_commit = types.IntType()
    entries = types.ListType(types.ModelType(LogEntry))


class AppendEntriesRPCResponse(MsgpackModel):
    term = types.IntType()
    success = types.BooleanType()
    index = types.IntType()


class RequestVoteRPCRequest(MsgpackModel):
    term = types.IntType()
    candidate_id = types.StringType()
    last_log_index = types.IntType()
    last_log_term = types.IntType()


class RequestVoteRPCResponse(MsgpackModel):
    term = types.IntType()
    success = types.BooleanType()


class RaftMessage(MsgpackModel):
    content = types.PolyModelType([AppendEntriesRPCRequest,
                                   AppendEntriesRPCResponse,
                                   RequestVoteRPCRequest,
                                   RequestVoteRPCRequest])

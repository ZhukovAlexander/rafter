from schematics import models, types
import msgpack


class MsgpackModel(models.Model):

    def pack(self):
        return msgpack.packb(self.to_primitive())

    @classmethod
    def unpack(cls, bytes):
        return cls({k: value for k, value in msgpack.unpackb(bytes, encoding='utf-8').items()})


class ArgsType(types.BaseType):
    """"""


class KwargsType(types.BaseType):

    """"""


class LogEntry(MsgpackModel):
    index = types.IntType()
    term = types.IntType()
    command = types.StringType()
    args = ArgsType()
    kwargs = KwargsType()


class AppendEntriesRPCRequest(MsgpackModel):
    term = types.IntType()
    leader_id = types.StringType()
    prev_log_index = types.IntType()
    prev_log_term = types.IntType()
    leader_commit = types.IntType()
    entries = types.ListType(types.ModelType(LogEntry))


class AppendEntriesRPCResponse(MsgpackModel):
    peer = types.StringType()
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


def claim_function(self, data):
    for model in RaftMessage.content.model_classes:
        if set(data).issubset(model.fields):
            return model


class RaftMessage(MsgpackModel):
    content = types.PolyModelType([AppendEntriesRPCRequest,
                                   AppendEntriesRPCResponse,
                                   RequestVoteRPCRequest,
                                   RequestVoteRPCRequest],
                                   claim_function=claim_function)

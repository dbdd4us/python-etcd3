import json

from google.protobuf import json_format

from etcd3.etcdrpc import RangeRequest, RangeResponse
from etcd3.etcdrpc import PutRequest, PutResponse
from etcd3.etcdrpc import DeleteRangeRequest, DeleteRangeResponse
from etcd3.etcdrpc import TxnRequest, TxnResponse
from etcd3.etcdrpc import CompactionRequest, CompactionResponse

from etcd3.etcdrpc import MemberAddRequest, MemberAddResponse
from etcd3.etcdrpc import MemberRemoveRequest, MemberRemoveResponse
from etcd3.etcdrpc import MemberUpdateRequest, MemberUpdateResponse
from etcd3.etcdrpc import MemberListRequest, MemberListResponse

from etcd3.etcdrpc import LeaseGrantRequest, LeaseGrantResponse
from etcd3.etcdrpc import LeaseRevokeRequest, LeaseRevokeResponse
from etcd3.etcdrpc import LeaseKeepAliveRequest, LeaseKeepAliveResponse
from etcd3.etcdrpc import LeaseTimeToLiveRequest, LeaseTimeToLiveResponse

from etcd3.etcdrpc import AlarmRequest, AlarmResponse
from etcd3.etcdrpc import StatusRequest, StatusResponse
from etcd3.etcdrpc import DefragmentRequest, DefragmentResponse
from etcd3.etcdrpc import HashRequest, HashResponse
from etcd3.etcdrpc import SnapshotRequest, SnapshotResponse

from etcd3.etcdrpc import WatchRequest, WatchResponse


class Stub(object):
    """for http-gateway
 
    """

    def __init__(self, client):
        """Constructor.
   
        Args:
          client: A etcd3.http_client.HTTPClient.
        """

        def _unary_unary_wrapper(path, request_serializer, response_deserializer):
            def _http(request_message, timeout):
                json_string = json_format.MessageToJson(request_message)
                response = client.post("{}{}".format(client.base_url, path), timeout=timeout, data=json_string)
                response.raise_for_status()
                return json_format.Parse(response.content, response_deserializer())
            return _http

        def _unary_stream_wrapper(path, request_serializer, response_deserializer):
            def _http(request_message, timeout):
                json_string = json_format.MessageToJson((request_message))
                response = client.post("{}{}".format(client.base_url, path), timeout=timeout, data=json_string)
                response.raise_for_status()
                try:
                    for line in response.iter_lines():
                        yield json_format.Parse(line, response_deserializer())
                finally:
                    response.close()
            return _http

        def _stream_stream_wrapper(path, request_serializer, response_deserializer):
            def _http(request_message_iter):
                # TODO(dbdd4us) find a way to use real stream upload here
                message = next(request_message_iter)
                json_string = json_format.MessageToJson((message))
                response = client.post("{}{}".format(client.base_url, path),
                                       data=json_string, stream=True, timeout=client.timeout)
                response.raise_for_status()
                try:
                    for line in response.iter_lines():
                        message = json.loads(line)
                        yield json_format.ParseDict(message["result"], response_deserializer())
                finally:
                    response.close()
            return _http


        self.Range = _unary_unary_wrapper("/kv/range", RangeRequest, RangeResponse)
        self.Put = _unary_unary_wrapper("/kv/put", PutRequest, PutResponse)
        self.DeleteRange = _unary_unary_wrapper("/kv/deleterange", DeleteRangeRequest, DeleteRangeResponse)
        self.Txn = _unary_unary_wrapper("/kv/txn", TxnRequest, TxnResponse)
        self.Compact = _unary_unary_wrapper("/kv/compaction", CompactionRequest, CompactionResponse)

        self.MemberAdd = _unary_unary_wrapper("/cluster/member", MemberAddRequest, MemberAddResponse)
        self.MemberRemove = _unary_unary_wrapper("/cluster/member", MemberRemoveRequest, MemberRemoveResponse)
        self.MemberUpdate = _unary_unary_wrapper("/cluster/member", MemberUpdateRequest, MemberUpdateResponse)
        self.MemberList = _unary_unary_wrapper("/cluster/member", MemberListRequest, MemberListResponse)

        self.LeaseGrant = _unary_unary_wrapper("lease/grant", LeaseGrantRequest, LeaseGrantResponse)
        self.LeaseRevoke = _unary_unary_wrapper("/kv/lease/revoke", LeaseRevokeRequest, LeaseRevokeResponse)
        self.LeaseKeepAlive = _stream_stream_wrapper("lease/keepalive", LeaseKeepAliveRequest, LeaseKeepAliveResponse)
        self.LeaseTimeToLive = _unary_unary_wrapper("/kv/lease/timetolive", LeaseTimeToLiveRequest, LeaseTimeToLiveResponse)

        self.Alarm = _unary_unary_wrapper("/maintenance/alarm", AlarmRequest, AlarmResponse)
        self.Status = _unary_unary_wrapper("/maintenance/status", StatusRequest, StatusResponse)
        self.Defragment = _unary_unary_wrapper("/maintenance/defragment", DefragmentRequest, DefragmentResponse)
        self.Hash = _unary_unary_wrapper("/maintenance/hash", HashRequest, HashResponse)
        self.Snapshot = _unary_stream_wrapper("/maintenance/snapshot", SnapshotRequest, SnapshotResponse)

        self.Watch = _stream_stream_wrapper("/watch", WatchRequest, WatchResponse)

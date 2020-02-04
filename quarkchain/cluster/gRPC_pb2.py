# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: gRPC.proto

import sys

_b = sys.version_info[0] < 3 and (lambda x: x) or (lambda x: x.encode("latin1"))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor.FileDescriptor(
    name="gRPC.proto",
    package="cluster",
    syntax="proto3",
    serialized_options=None,
    serialized_pb=_b(
        '\n\ngRPC.proto\x12\x07\x63luster"#\n!SetRootChainConfirmedBlockRequest"3\n\x12\x43lusterSlaveStatus\x12\x0c\n\x04\x63ode\x18\x01 \x01(\x05\x12\x0f\n\x07message\x18\x02 \x01(\t"Q\n"SetRootChainConfirmedBlockResponse\x12+\n\x06status\x18\x01 \x01(\x0b\x32\x1b.cluster.ClusterSlaveStatus2\x87\x01\n\x0c\x43lusterSlave\x12w\n\x1aSetRootChainConfirmedBlock\x12*.cluster.SetRootChainConfirmedBlockRequest\x1a+.cluster.SetRootChainConfirmedBlockResponse"\x00\x62\x06proto3'
    ),
)


_SETROOTCHAINCONFIRMEDBLOCKREQUEST = _descriptor.Descriptor(
    name="SetRootChainConfirmedBlockRequest",
    full_name="cluster.SetRootChainConfirmedBlockRequest",
    filename=None,
    file=DESCRIPTOR,
    containing_type=None,
    fields=[],
    extensions=[],
    nested_types=[],
    enum_types=[],
    serialized_options=None,
    is_extendable=False,
    syntax="proto3",
    extension_ranges=[],
    oneofs=[],
    serialized_start=23,
    serialized_end=58,
)


_CLUSTERSLAVESTATUS = _descriptor.Descriptor(
    name="ClusterSlaveStatus",
    full_name="cluster.ClusterSlaveStatus",
    filename=None,
    file=DESCRIPTOR,
    containing_type=None,
    fields=[
        _descriptor.FieldDescriptor(
            name="code",
            full_name="cluster.ClusterSlaveStatus.code",
            index=0,
            number=1,
            type=5,
            cpp_type=1,
            label=1,
            has_default_value=False,
            default_value=0,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
        ),
        _descriptor.FieldDescriptor(
            name="message",
            full_name="cluster.ClusterSlaveStatus.message",
            index=1,
            number=2,
            type=9,
            cpp_type=9,
            label=1,
            has_default_value=False,
            default_value=_b("").decode("utf-8"),
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
        ),
    ],
    extensions=[],
    nested_types=[],
    enum_types=[],
    serialized_options=None,
    is_extendable=False,
    syntax="proto3",
    extension_ranges=[],
    oneofs=[],
    serialized_start=60,
    serialized_end=111,
)


_SETROOTCHAINCONFIRMEDBLOCKRESPONSE = _descriptor.Descriptor(
    name="SetRootChainConfirmedBlockResponse",
    full_name="cluster.SetRootChainConfirmedBlockResponse",
    filename=None,
    file=DESCRIPTOR,
    containing_type=None,
    fields=[
        _descriptor.FieldDescriptor(
            name="status",
            full_name="cluster.SetRootChainConfirmedBlockResponse.status",
            index=0,
            number=1,
            type=11,
            cpp_type=10,
            label=1,
            has_default_value=False,
            default_value=None,
            message_type=None,
            enum_type=None,
            containing_type=None,
            is_extension=False,
            extension_scope=None,
            serialized_options=None,
            file=DESCRIPTOR,
        ),
    ],
    extensions=[],
    nested_types=[],
    enum_types=[],
    serialized_options=None,
    is_extendable=False,
    syntax="proto3",
    extension_ranges=[],
    oneofs=[],
    serialized_start=113,
    serialized_end=194,
)

_SETROOTCHAINCONFIRMEDBLOCKRESPONSE.fields_by_name[
    "status"
].message_type = _CLUSTERSLAVESTATUS
DESCRIPTOR.message_types_by_name[
    "SetRootChainConfirmedBlockRequest"
] = _SETROOTCHAINCONFIRMEDBLOCKREQUEST
DESCRIPTOR.message_types_by_name["ClusterSlaveStatus"] = _CLUSTERSLAVESTATUS
DESCRIPTOR.message_types_by_name[
    "SetRootChainConfirmedBlockResponse"
] = _SETROOTCHAINCONFIRMEDBLOCKRESPONSE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

SetRootChainConfirmedBlockRequest = _reflection.GeneratedProtocolMessageType(
    "SetRootChainConfirmedBlockRequest",
    (_message.Message,),
    {
        "DESCRIPTOR": _SETROOTCHAINCONFIRMEDBLOCKREQUEST,
        "__module__": "gRPC_pb2"
        # @@protoc_insertion_point(class_scope:cluster.SetRootChainConfirmedBlockRequest)
    },
)
_sym_db.RegisterMessage(SetRootChainConfirmedBlockRequest)

ClusterSlaveStatus = _reflection.GeneratedProtocolMessageType(
    "ClusterSlaveStatus",
    (_message.Message,),
    {
        "DESCRIPTOR": _CLUSTERSLAVESTATUS,
        "__module__": "gRPC_pb2"
        # @@protoc_insertion_point(class_scope:cluster.ClusterSlaveStatus)
    },
)
_sym_db.RegisterMessage(ClusterSlaveStatus)

SetRootChainConfirmedBlockResponse = _reflection.GeneratedProtocolMessageType(
    "SetRootChainConfirmedBlockResponse",
    (_message.Message,),
    {
        "DESCRIPTOR": _SETROOTCHAINCONFIRMEDBLOCKRESPONSE,
        "__module__": "gRPC_pb2"
        # @@protoc_insertion_point(class_scope:cluster.SetRootChainConfirmedBlockResponse)
    },
)
_sym_db.RegisterMessage(SetRootChainConfirmedBlockResponse)


_CLUSTERSLAVE = _descriptor.ServiceDescriptor(
    name="ClusterSlave",
    full_name="cluster.ClusterSlave",
    file=DESCRIPTOR,
    index=0,
    serialized_options=None,
    serialized_start=197,
    serialized_end=332,
    methods=[
        _descriptor.MethodDescriptor(
            name="SetRootChainConfirmedBlock",
            full_name="cluster.ClusterSlave.SetRootChainConfirmedBlock",
            index=0,
            containing_service=None,
            input_type=_SETROOTCHAINCONFIRMEDBLOCKREQUEST,
            output_type=_SETROOTCHAINCONFIRMEDBLOCKRESPONSE,
            serialized_options=None,
        ),
    ],
)
_sym_db.RegisterServiceDescriptor(_CLUSTERSLAVE)

DESCRIPTOR.services_by_name["ClusterSlave"] = _CLUSTERSLAVE

# @@protoc_insertion_point(module_scope)

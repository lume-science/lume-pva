# lume-pva

Lume-PVA is a Python library that serves or consumes EPICS PVs based on a LUMEModel subclass and its supported variables.

Features:
* Model outputs served over PVA, supporting a subset of the EPICS NormativeTypes metadata
* Inputs can be served as standalone PVs over PVA, or configured as clients for remote PVs
* Automatic discovery of remote PV protocols
* Snapshot mode for remote PVs

## Basic Usage

```py
from lume_pva.runner import Runner

myModel = MyLUMEModel()
r = Runner(model=myModel)
r.run()
```

## Operational Description

### Model Outputs

Model outputs are served over PVA, CA is not supported for these. If the output is solely an output, it will be configured as a read-only PV.

### Model Inputs

Model inputs can be configured as remote or local PVs. Local PVs are served by the Runner class and can be
interacted with using pvput or other PVA tools on the command line.

The only supported mode for local PVs is PVA, through p4p.

In the `remote` mode, the input is setup as a client. In this mode, both PVA and ChannelAccess (CA) are supported.

In `remote` mode, it is not necessary to specify the protocol of the remote PV. The `pvua` library will automatically detect
which protocol to use.

### Snapshot Mode

When configured in snapshot mode, the `Runner` will only fetch values from remote PVs when a snapshot is triggered by a write to
`{prefix}SNAPSHOT`.

`prefix` is passed to the constructor of the `Runner` class and defines a prefix to prepend to the start of PV names.

### Configuration

`Runner.generate_config()` can be used to generate a `dict` describing the default configuration for the model.
You can either edit this on the fly, or serialize it and edit it by hand later.

```py
print(Runner.generate_config(model=myModel))
```

An example configuration:
```py
{
    'remote_model_mode': 'continuous', # Set to 'snapshot' for snapshot mode
    'prefix': 'MY_PV_PREFIX:',
    'update_rate': 0.1, # Update period under which PVs will be batched together into one model.set(). Set to 0 to disable the window.
    'variables': {
        'input_a': {
            'name': 'input_a',
            'pv': 'input_a_pv',
            'mode': 'rw' # 'rw' means we can read and write this PV. It's served by the Runner class
        },
        'input_b': {
            'name': 'input_b',
            'pv': 'SOME:REMOTE:PV',
            'mode': 'remote' # 'remote' means this PV will be configured as a client and fetched from a remote
        }
    }
}
```

## Supported Variables

Supported variable types and their metadata fields.

### `ScalarVariable` and `IntVariable`

Represented as **NTScalar** with a `double` or `int` value field (depending on variable type).

Supported metadata:
* `timestamp`
* `display.units`
    * `ScalarVariable.unit`
* `control.limitLow`
    * `ScalarVariable.value_range[0]`
* `control.limitHigh`
    * `ScalarVariable.value_range[1]`
* `alarm.severity` and `alarm.status`
    * Set based on the value in relation to `value_range`. Out of range values trigger alarms.
    
### `NDVariable`

Represented as **NTNDArray** with data representation matching the numpy shape and dtype.

Supported metadata:
* `timestamp`

### `TorchScalarVariable`

Represented as **NTScalar** with a `double` value field.

Supported metadata:
* `timestamp`

### `TorchNDVariable`

Represented as **NTNDArray** with data representation matching the Tensor shape and dtype.

Supported metadata:
* `timestamp`

### `BoolVariable`

Represented as **NTScalar** with a `bool` value field.

Supported metadata:
* `timestamp`

### `StrVariable`

Represented as **NTScalar** with a `str` value field.

Supported metadata:
* `timestamp`
# lume-pva Runbook: From Zero to Running Models over EPICS

## For Newbies, By Newbies, Tested on Real Computers

**Platform:** Rocky Linux 10 | **Python:** 3.12 | **Machine:** lepton (Laptop)  
**Last tested:** July 2026 | **Branch:** `pr-fix-put-timeouts`

# Part 1: Concepts (Read This First)

## What is lume-pva?

lume-pva is a Python library that takes **any computational model** (math, ML, physics simulation) and makes its inputs and outputs accessible over a network using EPICS.

**Analogy:** Your model is a calculator. EPICS is the telephone system. lume-pva is the receptionist connecting calls to your calculator.

```
┌──────────────┐         ┌──────────────┐         ┌──────────────┐
│  YOUR MODEL  │◄───────►│   lume-pva   │◄───────►│   NETWORK    │
│  (Python)    │         │   (Runner)   │         │   (EPICS)    │
│              │         │              │         │              │
│  inputs      │         │  Serves PVs  │         │  pvget/pvput │
│  outputs     │         │  CA + PVA    │         │  camonitor   │
│  compute()   │         │              │         │  pvmonitor   │
└──────────────┘         └──────────────┘         └──────────────┘
```

---

## What is EPICS?

**EPICS** = Experimental Physics and Industrial Control System

A network protocol suite used worldwide in particle accelerators, telescopes, and large-scale scientific facilities. It lets programs share named data values over a network.

### Key Concept: Process Variables (PVs)

A **PV** is a named piece of data on the network. Think of it like a variable with a network address.

| Example PV Name | What it might be |
|---|---|
| `MATH:input_a` | A knob the user can turn |
| `MATH:sum_output` | A computed result |
| `FFT:fft_real` | A 1024-element array of FFT data |
| `BPM:X_POSITION` | Real beam position from hardware |

### Two Protocols: CA and PVA

| | **Channel Access (CA)** | **PV Access (PVA)** |
|---|---|---|
| **Era** | 1990s | 2010s |
| **Data types** | Scalars, simple arrays | Structured data, images, tables |
| **Search** | UDP broadcast | UDP multicast (or TCP name servers) |
| **Default port** | 5064 (server), 5065 (repeater) | 5075 (server), 5076 (search) |
| **Tools** | `caget`, `caput`, `camonitor` | `pvget`, `pvput`, `pvmonitor` |
| **Status** | Legacy but universal | Modern, preferred |

**lume-pva serves BOTH simultaneously.** Every PV is accessible via CA and PVA.

---

## How PV Discovery Works

When a client runs `pvget MATH:sum_output`, how does it FIND the server?

### PVA Discovery (Modern)

```
Client                          Network                         Server
  │                                                               │
  ├──UDP multicast search──────►  (EPICS_PVA_ADDR_LIST)           │
  │  "Who has MATH:sum_output?"                                   │
  │                                                               │
  │                              ◄──── Server sees search ────────┤
  │                                                               │
  │◄─────────── UDP response: "I have it! Connect to me on ──────┤
  │              port 37485"                                       │
  │                                                               │
  ├──TCP data connection────────────────────────────────────────► │
  │  (reads/writes PV values)                                     │
```

### CA Discovery (Legacy)

```
Client                     caRepeater              Server
  │                            │                      │
  ├──UDP search───────────────►│                      │
  │  (EPICS_CA_ADDR_LIST)      │                      │
  │                            │                      │
  │                            │◄──beacons────────────┤
  │◄──relays beacon────────────┤                      │
  │  "Server at port 45061"    │                      │
  │                                                   │
  ├──TCP connection──────────────────────────────────►│
```

### TCP Name Servers (Production/K8s)

For environments where UDP doesn't work (Kubernetes, SSH tunnels, firewalls):

```bash
# No UDP at all — purely TCP-based discovery
export EPICS_CA_ADDR_LIST=""
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_CA_NAME_SERVERS="server-ip:5064"

export EPICS_PVA_ADDR_LIST=""
export EPICS_PVA_AUTO_ADDR_LIST=NO
export EPICS_PVA_NAME_SERVERS="server-ip:5075"
```

Client opens a persistent TCP connection to the name server and sends searches over it. Works through SSH tunnels and across network boundaries.

---

## The caRepeater

**What:** A small background daemon that relays CA beacon messages.

**Why it matters:** When multiple CA servers run on one host, only the first gets the default port (5064). Others get ephemeral ports. Without caRepeater, clients can ONLY find the server on 5064. The repeater relays beacons from ALL servers (including those on ephemeral ports) to all clients.

```
WITHOUT caRepeater:
  Server A (port 5064) ── beacon ──► OK, clients find it
  Server B (port 45061) ── beacon ──► LOST, nobody listening

WITH caRepeater:
  Server A (port 5064) ── beacon ──► caRepeater ──► ALL clients
  Server B (port 45061) ── beacon ──► caRepeater ──► ALL clients
```

**Start it:** `caRepeater &` (once per machine, runs in background)

**PVA does NOT need a repeater** — its search mechanism handles multiple servers natively.

---

## Why Use Monitors (Not Polls)

### The Wrong Way: Poll

```python
while True:
    value = caget("sum_output")  # WRONG
    time.sleep(0.1)              # Wastes time, misses updates
```

**Problems:**
- You don't know when the model finishes computing
- You might read stale data
- You waste network bandwidth asking repeatedly
- You miss rapid transients between polls

### The Right Way: Monitor

```python
# Subscribe once, get notified on every change
camonitor("sum_output", callback=my_function)
# Your function fires ONLY when the value changes
# Zero wasted bandwidth, zero missed updates
```

**Why this matters for models:**
- Model evaluation time is UNPREDICTABLE (1ms for simple math, 30s for ML)
- There's no way to know "how long to sleep"
- A monitor tells you EXACTLY when the new value is ready

### The Complete Pattern: Monitor + Processing Status

```bash
# Terminal: Monitor outputs AND the processing flag
pvmonitor MATH:sum_output MATH:PROCESSING

# When you see:
#   PROCESSING  true        ← model is computing
#   sum_output  8.0         ← result posted
#   PROCESSING  false       ← safe to use the value
```

---

## Why Models Need a PROCESSING/BUSY Status PV

| Scenario | Without status PV | With status PV |
|---|---|---|
| Client writes input, reads output | Might get stale value | Wait for PROCESSING=false, guaranteed fresh |
| Multiple inputs batched | Don't know when batch is done | PROCESSING=false means ALL inputs processed |
| Slow model (ML inference) | Client guesses with sleep | Client knows exactly when done |
| Multiple clients | Chaos | Everyone monitors same status |

**This is the BUSY record pattern used everywhere in EPICS:**
- Motor: BUSY=1 during motion → BUSY=0 when settled
- Detector: BUSY=1 during acquisition → BUSY=0 when data ready
- Model: PROCESSING=1 during eval → PROCESSING=0 when outputs posted

---

## Avoiding PV Name Collisions

**Rule:** Every PV name on the network must be UNIQUE.

**How:** Use prefixes.

```python
# WRONG — two models both serve "model_info"
Runner.generate_config(model)  # prefix=""

# RIGHT — unique per model
config["prefix"] = "MATH:"   # → MATH:model_info, MATH:input_a
config["prefix"] = "FFT:"    # → FFT:model_info, FFT:fft_real
```

When two servers expose the same PV name, clients get whichever server responds first — unpredictable and wrong.

---

## Port Assignment for Multiple Servers

```
First server started  → gets default ports (5064 CA, 5075 PVA)
Second server started → OS assigns ephemeral ports (e.g., 45061, 41309)
Third server started  → OS assigns different ephemeral ports

Clients don't need to know ports — they search by PV NAME.
The discovery protocol handles finding the right server.
```

---

# Part 2: Installation

## Prerequisites

- Rocky Linux 10 (or any modern Linux)
- Internet access (for downloading packages)
- git (for cloning the repo)

## Step 1: Install Anaconda

```bash
# Find latest version
cd ~/Downloads
curl -s https://repo.anaconda.com/archive/ | grep "Anaconda3.*Linux-x86_64" | head -5

# Download (replace version as needed)
curl -O https://repo.anaconda.com/archive/Anaconda3-2026.07-1-Linux-x86_64.sh

# Install
bash Anaconda3-2026.07-1-Linux-x86_64.sh
# Accept license: yes
# Install location: press Enter (default ~/anaconda3 or custom)
# Initialize conda: yes

# Reload shell
source ~/.bashrc
```

## Step 2: Configure Conda (Keep Your Shell Clean)

```bash
# Show conda env in prompt ONLY when activated
conda config --set changeps1 true

# Don't auto-activate base on every new terminal
conda config --set auto_activate_base false

# Verify
cat ~/.condarc
# Should show:
#   changeps1: true
#   auto_activate_base: false
```

**Behavior after this:**

| State | Prompt |
|---|---|
| New terminal (no env) | `ernesto@lepton:~$` |
| After `conda activate lume-pva` | `(lume-pva) ernesto@lepton:~$` |
| After `conda deactivate` | `ernesto@lepton:~$` |

## Step 3: Clone the Repository

```bash
cd ~/controls/lume-pva
git clone https://github.com/lume-science/lume-pva.git lume-pva-src
cd lume-pva-src

# Optionally switch to a specific branch
git checkout pr-fix-put-timeouts
```

## Step 4: Create Conda Environment

```bash
conda create -n lume-pva python=3.12 -y
```

## Step 5: Activate and Install

```bash
conda activate lume-pva

# Install EPICS command-line tools (pvget, caget, etc.)
conda install -c conda-forge epics-base -y

# Install lume-pva with development dependencies
cd ~/controls/lume-pva/lume-pva-src
pip install -e ".[dev]"
```

### What `pip install -e ".[dev]"` means:

| Part | Meaning |
|---|---|
| `pip install` | Install a Python package |
| `-e` | Editable mode — symlinks to your source, changes are instant |
| `.` | Current directory (reads pyproject.toml) |
| `[dev]` | Also install optional dev extras (pytest, p4p, pyepics) |

### Editable mode explained:

```
EDITABLE (pip install -e .):
  site-packages/ has a POINTER to your source folder
  Edit source → change is instant → no reinstall needed

NORMAL (pip install .):
  site-packages/ has a COPY of your source
  Edit source → nothing happens → must reinstall
```

## Step 6: Verify Installation

```bash
python -c "import lume_pva; print('lume_pva OK')"
python -c "import p4p; print('p4p OK')"
python -c "import pcaspy; print('pcaspy OK')"
python -c "import numpy; print('numpy OK')"
python -c "import pvua; print('pvua OK')"
python -c "import epics; print('pyepics OK')"
which pvget
which caget
```

All should pass. Note: the pip package is called `pyepics` but the import is `import epics` (common Python quirk).

## Step 7: Set EPICS Environment for Local Development

```bash
# Add to ~/.bashrc for persistence, or run per-session:
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
```

This keeps all EPICS traffic on localhost — no broadcasts to the lab network.

---

# Part 3: Running the Examples

## Install tmux (Terminal Management)

```bash
sudo dnf install tmux -y
```

### tmux Quick Reference

| Action | Command |
|---|---|
| Create named session | `tmux new-session -s name` |
| Detach (keeps running) | `Ctrl+b` then `d` |
| List sessions | `tmux ls` |
| Attach to session | `tmux attach -t name` |
| Kill session | `tmux kill-session -t name` |
| Kill ALL sessions | `tmux kill-server` |
| Create window (inside tmux) | `Ctrl+b` then `c` |
| Rename window | `Ctrl+b` then `,` |
| Next/prev window | `Ctrl+b` then `n`/`p` |

---

## Example 1: Math Model (Simplest Case)

### What it does:

```
Inputs:  input_a (float), input_b (float), input_c (float), input_d (int), invert (bool)
Logic:   sum_output = input_a + input_b + input_c + input_d
         if invert: sum_output = -sum_output
Output:  sum_output (float), desc (string), my_enum (enum)
```

### Start the server:

```bash
tmux new-session -s math-model

# Inside tmux:
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
caRepeater &
cd ~/controls/lume-pva/lume-pva-src
python examples/math_model.py

# Detach: Ctrl+b then d
```

### Interact from a client terminal:

```bash
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO

# Read values (PVA)
pvget sum_output          # → 4.0 (default: 1+1+1+1)
pvget input_a             # → 1.0

# Write values (PVA)
pvput input_a 5.0         # Set input_a to 5

# Monitor (see updates in real-time)
pvmonitor sum_output input_a input_b input_c

# In another terminal, write values and watch the monitor update:
pvput input_b 3.0
pvput input_c 2.0
pvput invert true         # Note: booleans need "true"/"false", not 1/0

# CA equivalents
caget sum_output
caput input_a 7.0
camonitor sum_output

# Reset model to defaults
pvput RESET 0

# View model info (what PVs are available)
pvget model_info
```

### Variable types reference:

| PV | Type | Read/Write | Values |
|---|---|---|---|
| `input_a` | float | rw | range: -10 to 10 |
| `input_b` | float | rw | range: -10 to 10 |
| `input_c` | float | rw | range: -10 to 10 |
| `input_d` | int | rw | range: -10 to 10 |
| `invert` | bool | rw | `true` / `false` |
| `desc` | string | ro | "Hello, world!" |
| `sum_output` | float | ro | computed |
| `my_enum` | enum | rw | test1, test2, test3, hello |
| `RESET` | control | write | any write triggers reset |
| `SNAPSHOT` | control | write | triggers remote PV snapshot |
| `model_info` | struct | ro | model metadata |

### Alarms:

When a value goes outside its `value_range`, EPICS alarms trigger:

```bash
pvput input_c 12.0    # Outside range (-10, 10)
camonitor input_c     # Shows: "input_c  12 HIHI MAJOR"
```

---

## Example 2: FFT Model (Array PVs + Remote Inputs)

### What it does:

```
Architecture:
  SimpleSimulator → generates 3 sine wave signals (arrays of 1024 points)
  FFTModel        → subscribes to those signals, computes FFT, publishes result

Simulator outputs (served locally):
  signal_a = 4 * sin(2π * t)        (1024 samples)
  signal_b = 2.1 * sin(4.3π * t)    (1024 samples)
  signal_c = 3.3 * sin(0.5544π * t) (1024 samples)

Model computation:
  combined_signal = signal_a + signal_b + signal_c
  fft_result = FFT(combined_signal)

Model outputs (served as PVs):
  fft_real = real part of FFT (1024 points)
  fft_imag = imaginary part of FFT (1024 points)
```

### Start the server:

```bash
tmux new-session -s fft-model

# Inside tmux:
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src
python examples/fft_model.py

# Detach: Ctrl+b then d
```

### Interact:

```bash
# Read FFT output (1024-element array)
pvget fft_real
pvget fft_imag

# Read simulator signals
pvget signal_a
pvget signal_b
pvget signal_c

# Monitor — watch FFT update continuously as signals change
pvmonitor fft_real
```

### Architecture (single process, two servers):

```
PID: python examples/fft_model.py
┌─────────────────────────────────────────────────────────────┐
│  SERVER A: SimpleSimulator                                   │
│  Purpose: Generates fake sine wave signals (test data)       │
│  CA port:  ephemeral (e.g., 45061)                           │
│  PVA port: ephemeral (e.g., 41309)                           │
│  Serves: signal_a, signal_b, signal_c                        │
├─────────────────────────────────────────────────────────────┤
│  SERVER B: Runner (FFTModel)                                 │
│  Purpose: Subscribes to signals, computes FFT, serves result │
│  CA port:  ephemeral (e.g., 44485)                           │
│  PVA port: ephemeral (e.g., 37485)                           │
│  Subscribes to: signal_a, signal_b, signal_c (from above)   │
│  Serves: fft_real, fft_imag, string_array, 2d_array         │
└─────────────────────────────────────────────────────────────┘
```

---

## Running Both Examples Simultaneously

```bash
# Start math-model in its own tmux session
tmux new-session -s math-model
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
caRepeater &
cd ~/controls/lume-pva/lume-pva-src
python examples/math_model.py
# Ctrl+b, d to detach

# Start fft-model in its own tmux session
tmux new-session -s fft-model
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src
python examples/fft_model.py
# Ctrl+b, d to detach
```

### Verify both are running:

```bash
tmux ls
# math-model: 1 windows (created ...)
# fft-model: 1 windows (created ...)

# Check ports
ss -tlnp | grep python
# Shows: 5064, 5075 (math-model, default ports)
#        45061, 44485, 41309, 37485 (fft-model, ephemeral)

# Talk to both
pvget sum_output    # from math-model
pvget fft_real      # from fft-model

# Monitor both simultaneously
pvmonitor sum_output fft_real
```

### Full port map when both are running:

```
HOST: lepton
═══════════════════════════════════════════════════════════════════

PID 77596 — python examples/math_model.py
┌─────────────────────────────────────────────────────────────┐
│  Runner (SimpleMathModel)                                    │
│  Purpose: Serves a simple a+b+c+d model over EPICS          │
│                                                              │
│  CA port:  5064 (default)                                    │
│  PVA port: 5075 (default)                                    │
│                                                              │
│  Serves PVs:                                                 │
│    input_a (rw, float)     input_b (rw, float)               │
│    input_c (rw, float)     input_d (rw, int)                 │
│    invert (rw, bool)       my_enum (rw, enum)                │
│    desc (ro, string)       sum_output (ro, float)            │
│    model_info (ro)         RESET (control)                   │
│    SNAPSHOT (control)                                         │
└─────────────────────────────────────────────────────────────┘

PID 77744 — python examples/fft_model.py
┌─────────────────────────────────────────────────────────────┐
│  SERVER A: SimpleSimulator                                   │
│  Purpose: Generates fake sine wave signals (test data)       │
│                                                              │
│  CA port:  45061 (ephemeral)                                 │
│  PVA port: 41309 (ephemeral)                                 │
│                                                              │
│  Serves PVs:                                                 │
│    signal_a (array1d, expr: 4*sin(2*pi*t))                   │
│    signal_b (array1d, expr: 2.1*sin(4.3*pi*t))              │
│    signal_c (array1d, expr: 3.3*sin(0.5544*pi*t))           │
├─────────────────────────────────────────────────────────────┤
│  SERVER B: Runner (FFTModel)                                 │
│  Purpose: Subscribes to signals, computes FFT, serves result │
│                                                              │
│  CA port:  44485 (ephemeral)                                 │
│  PVA port: 37485 (ephemeral)                                 │
│                                                              │
│  Remote (subscribes to):                                     │
│    signal_a, signal_b, signal_c (from Simulator above)       │
│                                                              │
│  Serves PVs:                                                 │
│    fft_real (ro, array)    fft_imag (ro, array)              │
│    string_array (ro)       2d_array (ro)                     │
│    model_info (ro)         RESET (control)                   │
│    SNAPSHOT (control)                                         │
└─────────────────────────────────────────────────────────────┘
```

---

## Example 3: Math Model in Remote/Snapshot Mode

The math model can also run with remote inputs (subscribing to PVs from another source):

```bash
# Remote mode: model subscribes to external PVs continuously
python examples/math_model.py --mode remote

# Snapshot mode: model only fetches remote PVs when SNAPSHOT is triggered
python examples/math_model.py --mode snapshot
```

In remote mode, a SimpleSimulator is also started to provide fake input values.

---

# Part 4: Create Your Own Model

## The Template

Every lume-pva model must:
1. Inherit from `LUMEModel`
2. Define `supported_variables` (inputs + outputs)
3. Implement `_get()`, `_set()`, and `reset()`

```python
#!/usr/bin/env python3
"""
my_model.py — Your custom model served over EPICS

Computes: output = gain * input_signal + offset
"""
from typing import Any
from lume.model import LUMEModel
from lume.variables import ScalarVariable
from lume_pva.runner import Runner


class MyModel(LUMEModel):
    """A simple gain + offset model."""

    def __init__(self):
        # Initial state for all variables
        self._initial_state = {
            "input_signal": 0.0,
            "gain": 1.0,
            "offset": 0.0,
            "output": 0.0,
        }
        self._state = self._initial_state.copy()

        # Define variables: inputs (read_only=False) and outputs (read_only=True)
        self._variables = {
            "input_signal": ScalarVariable(
                name="input_signal",
                default_value=0.0,
                value_range=(-100.0, 100.0),
                unit="V",
                read_only=False,  # Users can write to this
            ),
            "gain": ScalarVariable(
                name="gain",
                default_value=1.0,
                value_range=(0.0, 100.0),
                unit="dimensionless",
                read_only=False,
            ),
            "offset": ScalarVariable(
                name="offset",
                default_value=0.0,
                value_range=(-50.0, 50.0),
                unit="V",
                read_only=False,
            ),
            "output": ScalarVariable(
                name="output",
                default_value=0.0,
                unit="V",
                read_only=True,  # Computed, not user-settable
            ),
        }

    @property
    def supported_variables(self) -> dict[str, ScalarVariable]:
        return self._variables

    def _get(self, names: list[str]) -> dict[str, Any]:
        """Return current values for requested variables."""
        return {name: self._state[name] for name in names}

    def _set(self, values: dict[str, Any]) -> None:
        """Set inputs and recompute outputs."""
        # Update inputs
        for name, value in values.items():
            self._state[name] = value

        # Compute output
        self._state["output"] = (
            self._state["gain"] * self._state["input_signal"] + self._state["offset"]
        )

    def reset(self) -> None:
        """Reset to initial state."""
        self._state = self._initial_state.copy()


# --- Main entry point ---
if __name__ == "__main__":
    import argparse
    import logging

    parser = argparse.ArgumentParser()
    parser.add_argument("-v", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG if args.v else logging.INFO)

    # Create model instance
    model = MyModel()

    # Generate config and set prefix (ALWAYS use a prefix!)
    config = Runner.generate_config(model, prefix="MYMODEL:")

    # Optional: customize config
    config["description"] = "Simple gain + offset model"
    # config["update_rate"] = 0.1  # Batching window in seconds

    # Create runner and block forever
    runner = Runner(model=model, config=config)
    runner.run()
```

## Run Your Model

```bash
cd ~/controls/lume-pva/lume-pva-src
python my_model.py

# From another terminal:
pvget MYMODEL:output           # → 0.0
pvput MYMODEL:input_signal 5.0
pvput MYMODEL:gain 2.0
pvput MYMODEL:offset 1.0
pvget MYMODEL:output           # → 11.0 (2*5 + 1)
pvput MYMODEL:RESET 0
pvget MYMODEL:output           # → 0.0 (back to defaults)
```

## Checklist for Your Model

- [ ] Inherit from `LUMEModel`
- [ ] Define all variables with `name`, `read_only`, `value_range`, `unit`
- [ ] Mark inputs as `read_only=False`
- [ ] Mark outputs as `read_only=True`
- [ ] Implement `_set()` — update inputs, compute outputs, store in `self._state`
- [ ] Implement `_get()` — return requested values from `self._state`
- [ ] Implement `reset()` — restore initial state
- [ ] Use a unique `prefix` in your Runner config
- [ ] Set `value_range` on inputs — enables EPICS alarms automatically

## Available Variable Types

| Type | Python type | EPICS type | Use for |
|---|---|---|---|
| `ScalarVariable` | float | NTScalar (double) | Continuous values (temperature, position) |
| `IntVariable` | int | NTScalar (long) | Integer values (counts, indices) |
| `BoolVariable` | bool | NTScalar (boolean) | Flags (on/off, enabled/disabled) |
| `StrVariable` | str | NTScalar (string) | Labels, descriptions |
| `EnumVariable` | str/int | NTEnum | Fixed choices (mode selection) |
| `NDVariable` | numpy array | NTNDArray | Images, waveforms, spectra |
| `TorchScalarVariable` | tensor | NTScalar (double) | ML model scalars |
| `TorchNDVariable` | tensor | NTNDArray | ML model arrays |

---

# Part 5: Reference

## Daily Workflow

```bash
# New terminal
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO

# Check what's running
tmux ls

# Attach to a server
tmux attach -t math-model

# Start a new server
tmux new-session -s my-model
python my_model.py
# Ctrl+b, d to detach
```

## EPICS Environment Variables

| Variable | Purpose | Local dev value |
|---|---|---|
| `EPICS_CA_ADDR_LIST` | Where to send CA searches | `127.0.0.1` |
| `EPICS_CA_AUTO_ADDR_LIST` | Auto-detect broadcast addresses | `NO` |
| `EPICS_CA_NAME_SERVERS` | TCP-based CA name resolution | (not set locally) |
| `EPICS_PVA_ADDR_LIST` | Where to send PVA searches | `127.0.0.1` |
| `EPICS_PVA_AUTO_ADDR_LIST` | Auto-detect multicast addresses | `NO` |
| `EPICS_PVA_NAME_SERVERS` | TCP-based PVA name resolution | (not set locally) |

## Common Commands

| Action | PVA command | CA command |
|---|---|---|
| Read a PV | `pvget pv_name` | `caget pv_name` |
| Write a PV | `pvput pv_name value` | `caput pv_name value` |
| Monitor (live stream) | `pvmonitor pv_name` | `camonitor pv_name` |
| Read structured PV | `pvget -v pv_name` | (not supported) |

## tmux Management

| Action | Command |
|---|---|
| List sessions | `tmux ls` |
| Attach | `tmux attach -t name` |
| Detach (from inside) | `Ctrl+b` then `d` |
| Kill one session | `tmux kill-session -t name` |
| Kill everything | `tmux kill-server` |

## Troubleshooting

| Problem | Fix |
|---|---|
| `CA beacon "Connection refused"` | Start `caRepeater &` |
| `pvget` can't find PV | Check EPICS env vars, check server is running |
| `caput` shows stale "New" value | Known issue — use monitors instead of readback |
| Timestamps show 1970 | Update to latest branch (timestamp bug was fixed) |
| PV name collision | Add unique prefix to each Runner config |
| `conda: command not found` | `source ~/.bashrc` |
| `(base)` stuck in prompt | `conda deactivate` or open new terminal |
| Want to nuke conda env | `conda env remove -n lume-pva` then recreate |

## Project File Map

```
lume-pva-src/
├── examples/
│   ├── math_model.py      ← Start here! Simple a+b+c+d
│   ├── fft_model.py       ← Array PVs, remote inputs, FFT
│   └── simulator.py       ← Standalone signal generator
├── lume_pva/
│   ├── epics.py           ← Alarm severity/status enums (tiny)
│   ├── runner.py          ← THE BRAIN — connects models to EPICS
│   ├── simulator.py       ← SimpleSimulator class (fake PV data)
│   ├── variables.py       ← Type handlers (Python ↔ EPICS conversion)
│   └── tests/
│       ├── test_runner_epics.py  ← End-to-end EPICS tests
│       ├── test_runner.py        ← Config generation tests
│       └── test_variables.py     ← Variable type tests
├── pyproject.toml          ← Project config + dependencies
└── README.md               ← Project overview
```

## Key Design Principles

1. **ALWAYS use a prefix** — prevents PV name collisions
2. **ALWAYS use monitors** — never poll with caget/pvget in loops
3. **Model logic goes in `_set()`** — Runner handles all EPICS plumbing
4. **Mark outputs as `read_only=True`** — prevents clients from corrupting computed values
5. **Set `value_range` on inputs** — gives you free EPICS alarms
6. **One tmux session per server** — clean separation, survives logout
7. **caRepeater for multi-server CA** — start once per machine


# Part 6: Gaussian Signal Processing Pipeline

## Overview

A two-process signal processing pipeline demonstrating:
- A **simulator** generating noisy Gaussian beam profiles (like a wire scanner)
- A **classical denoiser** estimating beam position and size from noisy data using curve fitting

### Real-World Analogy

This is exactly what happens with beam profile monitors in accelerators:
- A wire scanner or screen measures the transverse beam profile
- The true profile is approximately Gaussian (mean = beam position, sigma = beam size)
- Electronics and digitization add noise
- We need to extract the true beam position and size from the noisy measurement

```
TRUE SIGNAL         + NOISE           = MEASURED
    ████               ░░░                █░██░
   ██████          ░░░░░░░░░░           ░██░████░
  ████████         ░░░░░░░░░░          ░░████████░░
 ██████████        ░░░░░░░░░░         ░░██████████░░
Gaussian(μ,σ)      N(0, noise)        What detector sees
```

### Architecture: Independent Processes

```
┌──────────────────────────────────┐         ┌─────────────────────────────────┐
│  gaussian_sim.py                 │         │  gaussian_classical.py           │
│  tmux: gauss-sim                 │  PVA    │  tmux: gauss-denoise             │
│                                  │ monitor │                                  │
│  Generates:                      │         │  Subscribes to:                  │
│    SIM:noisy_signal (256-pt arr) ├────────►│    SIM:noisy_signal              │
│    SIM:clean_signal (256-pt arr) │         │    SIM:x_axis                    │
│    SIM:x_axis       (256-pt arr) ├────────►│                                  │
│    SIM:mean  (true center)       │         │  Computes (scipy curve_fit):     │
│    SIM:sigma (true width)        │         │    est_mean      (estimated μ)   │
│    SIM:snr   (signal-to-noise)   │         │    est_sigma     (estimated σ)   │
│                                  │         │    est_amplitude (estimated A)   │
│  PVA port: ephemeral             │         │    denoised_signal (fit result)  │
│  CA:  none (p4p only)            │         │    fit_quality   (R², 0-1)       │
│                                  │         │                                  │
│  Parameters wander over time:    │         │  CA port:  ephemeral             │
│    mean  = 3*sin(0.2*t)          │         │  PVA port: ephemeral             │
│    sigma = 2 + sin(0.05*t)       │         │                                  │
└──────────────────────────────────┘         └─────────────────────────────────┘
        INDEPENDENT PROCESS                          INDEPENDENT PROCESS
```

### Why Independent Processes (Not Same-Process)

| | **Same Process (fft_model.py style)** | **Independent (gaussian style)** |
|---|---|---|
| **Reusability** | ❌ Simulator dies when model dies | ✅ Simulator feeds ANY number of models |
| **Fault Isolation** | ❌ Crash in model kills simulator | ✅ Model crash doesn't affect data source |
| **Testing** | ❌ Can't test simulator alone | ✅ Can run and validate each piece independently |
| **Scalability** | ❌ Tight coupling, one consumer | ✅ Multiple models subscribe to same simulator |
| **Production-like** | ❌ Not how real hardware works | ✅ Matches real EPICS (IOC serves, apps consume) |
| **Debugging** | ❌ Harder to isolate issues | ✅ Can stop/restart each piece independently |

**Rule of thumb:** In production, always use independent processes. The same-process pattern is only acceptable for quick demos.

---

## Example 4: Gaussian Simulator (`gaussian_sim.py`)

### What it does:

```
Generates a noisy Gaussian signal that simulates a beam profile measurement.

Signal:    y(x) = amplitude * exp(-0.5 * ((x - mean) / sigma)²) + noise
           where noise ~ N(0, noise_level)

The mean and sigma parameters wander over time (sinusoidal drift)
to simulate a beam that slowly moves and changes size.

X-axis:    256 points from -10 to +10 (millimeters)
Update rate: every 0.5 seconds
```

### How it works (no LUMEModel, no Runner):

This simulator uses `p4p` directly — it's a simple loop that generates data and posts it to PVA SharedPVs. No Runner, no model class, no validation overhead.

```python
# Simplified logic:
while True:
    mean = 3.0 * sin(0.2 * t)           # Beam drifts left/right
    sigma = 2.0 + 1.0 * sin(0.05 * t)   # Beam size oscillates
    clean = amplitude * exp(-0.5 * ((x - mean) / sigma)²)
    noisy = clean + random_noise(0, noise_level, 256)
    snr = 10 * log10(signal_power / noise_power)
    post_to_pvs(noisy, clean, mean, sigma, snr)
    sleep(0.5)
```

### Start the simulator:

```bash
tmux new-session -s gauss-sim

# Inside tmux:
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src
python examples/gaussian_sim.py

# Expected output:
# Gaussian Simulator running
#   PVs: SIM:noisy_signal, SIM:clean_signal, SIM:x_axis
#         SIM:mean, SIM:sigma, SIM:snr
#   Try:  pvmonitor SIM:mean SIM:sigma SIM:snr

# Detach: Ctrl+b then d
```

### Interact with the simulator:

```bash
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO

# Watch parameters drift in real-time
pvmonitor SIM:mean SIM:sigma SIM:snr

# Read the noisy signal (256-point array)
pvget SIM:noisy_signal

# Read the clean signal (ground truth)
pvget SIM:clean_signal
```

### PV Reference (Simulator):

| PV Name | Type | Description |
|---|---|---|
| `SIM:noisy_signal` | array (256 floats) | Gaussian + white noise (what a detector sees) |
| `SIM:clean_signal` | array (256 floats) | True Gaussian without noise (ground truth) |
| `SIM:x_axis` | array (256 floats) | X coordinates: -10.0 to +10.0 mm |
| `SIM:mean` | float | True beam center position (mm) |
| `SIM:sigma` | float | True beam width (mm) |
| `SIM:snr` | float | Signal-to-noise ratio (dB) |

### Important notes:

- **PVA only** — this simulator uses `p4p` directly, NOT the Runner. It does not serve CA PVs.
  Use `pvget`/`pvmonitor`, NOT `caget`/`camonitor`.
- **No prefix bug** — since it doesn't use the Runner, it doesn't hit the prefix issue.
- **Read-only** — all PVs are published by the server. Clients cannot write to them.

---

## Example 5: Classical Gaussian Denoiser (`gaussian_classical.py`)

### What it does:

```
Subscribes to the simulator's noisy signal, fits a Gaussian using
scipy.optimize.curve_fit, and publishes the estimated parameters.

Input (from simulator via EPICS monitor):
  SIM:noisy_signal  — the noisy 256-point array
  SIM:x_axis        — the x-coordinates

Processing (scipy curve_fit):
  Minimize: Σ (y_measured[i] - A*exp(-0.5*((x[i]-μ)/σ)²))²
  Over parameters: A (amplitude), μ (mean), σ (sigma)

Output (served as EPICS PVs):
  est_mean       — estimated beam center (mm)
  est_sigma      — estimated beam width (mm)
  est_amplitude  — estimated peak height
  denoised_signal — reconstructed clean Gaussian from fit
  fit_quality    — R² goodness of fit (0.0 to 1.0)
```

### How it works (uses LUMEModel + Runner):

This denoiser follows the standard lume-pva pattern: a `LUMEModel` subclass with a `Runner` that handles all EPICS plumbing.

```python
# Simplified logic inside _set():
def _set(self, values):
    # Update state with received signals
    for name, value in values.items():
        self._state[name] = value

    x = self._state["x_axis"]
    y = self._state["noisy_signal"]

    # Fit Gaussian: y ≈ A * exp(-0.5 * ((x - μ) / σ)²)
    popt, _ = curve_fit(gaussian, x, y, p0=[max(y), center_of_mass, 1.0])
    est_amp, est_mean, est_sigma = popt

    # Store results
    self._state["est_mean"] = float(est_mean)
    self._state["est_sigma"] = float(abs(est_sigma))
    self._state["est_amplitude"] = float(est_amp)
    self._state["denoised_signal"] = gaussian(x, *popt)
    self._state["fit_quality"] = R_squared(y, denoised)
```

### Start the denoiser:

**Prerequisite:** The simulator (`gauss-sim`) must already be running.

```bash
tmux new-session -s gauss-denoise

# Inside tmux:
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src
python examples/gaussian_classical.py

# Expected output:
# Classical Denoiser running (prefix=none)
#   Subscribes to: SIM:noisy_signal, SIM:x_axis
#   Serves: est_mean, est_sigma, est_amplitude
#           denoised_signal, fit_quality
#   Try:  pvmonitor est_mean est_sigma fit_quality

# Detach: Ctrl+b then d
```

### Monitor results (compare truth vs estimate):

```bash
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO

# Side-by-side comparison: true values vs denoiser estimates
pvmonitor est_mean est_sigma fit_quality SIM:mean SIM:sigma
```

### Expected output:

```
est_mean    2026-07-31 10:02:06.904  -0.625976
est_sigma   2026-07-31 10:02:06.904  1.32242
fit_quality 2026-07-31 10:02:06.904  0.900502
SIM:mean    2026-07-31 10:02:07.403  -0.685766
SIM:sigma   2026-07-31 10:02:07.403  1.33482
est_mean    2026-07-31 10:02:07.405  -0.662341
est_sigma   2026-07-31 10:02:07.405  1.36451
fit_quality 2026-07-31 10:02:07.405  0.897985
```

**Performance:**
- `est_mean` tracks `SIM:mean` within ~0.05 mm
- `est_sigma` tracks `SIM:sigma` within ~0.05 mm
- `fit_quality` ≈ 0.90 (R² of 90% — correct for SNR ~13 dB)

### PV Reference (Denoiser):

| PV Name | Type | R/W | Description |
|---|---|---|---|
| `est_mean` | float | ro | Estimated beam center (mm) |
| `est_sigma` | float | ro | Estimated beam width (mm) |
| `est_amplitude` | float | ro | Estimated peak height (a.u.) |
| `denoised_signal` | array (256) | ro | Reconstructed clean Gaussian |
| `fit_quality` | float | ro | R² goodness of fit (0.0 = terrible, 1.0 = perfect) |
| `RESET` | control | write | Reset model to defaults |
| `SNAPSHOT` | control | write | (unused in continuous mode) |
| `model_info` | struct | ro | Model metadata |

### Known Issues:

- **Prefix bug:** Using `prefix="DENOISE:"` causes `Simulation Cycle Failed` errors.
  Workaround: use `prefix=""` until the bug is fixed in the Runner.
- **OptimizeWarning:** Occasionally `curve_fit` warns about covariance estimation.
  This is harmless — it means the fit succeeded but the error bars couldn't be computed.
- **No CA on simulator:** The simulator (`gaussian_sim.py`) uses p4p directly.
  You must use `pvget`/`pvmonitor` to read simulator PVs, not `caget`/`camonitor`.
  The denoiser Runner does serve both CA and PVA for its output PVs.

---

## Running the Full Gaussian Pipeline

### Step-by-step startup:

```bash
# Step 1: Start the simulator
tmux new-session -s gauss-sim
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src
python examples/gaussian_sim.py
# Wait for "Gaussian Simulator running" message
# Detach: Ctrl+b then d

# Step 2: Start the denoiser
tmux new-session -s gauss-denoise
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src
python examples/gaussian_classical.py
# Wait for "Classical Denoiser running" message (no errors!)
# Detach: Ctrl+b then d

# Step 3: Verify
tmux ls
# Should show: gauss-sim and gauss-denoise

# Step 4: Monitor
pvmonitor est_mean est_sigma fit_quality SIM:mean SIM:sigma
```

### Shutdown:

```bash
# Kill one at a time
tmux kill-session -t gauss-denoise
tmux kill-session -t gauss-sim

# Or kill everything
tmux kill-server
```

### Startup order matters:

The denoiser subscribes to `SIM:noisy_signal` and `SIM:x_axis`. If the simulator
isn't running when the denoiser starts, the Runner will wait for those PVs to appear.
Once the simulator starts, the denoiser will automatically connect and begin processing.

**Best practice:** Start the simulator first, wait 2-3 seconds, then start the denoiser.

---

## Server Discovery and Port Mapping

### The Problem

Every time a server restarts, the OS assigns different ephemeral ports.
You can't hardcode port numbers. You need a way to discover what's running.

### Quick Discovery Commands

```bash
# 1. What tmux sessions exist?
tmux ls

# 2. What Python EPICS servers are running, with PIDs and commands?
ps aux | grep "python examples" | grep -v grep

# 3. What ports are those PIDs listening on?
ss -tlnp | grep python

# 4. Full combined view (one-liner):
echo "=== SESSIONS ===" && tmux ls && echo "" && echo "=== PROCESSES ===" && ps aux | grep "python examples" | grep -v grep && echo "" && echo "=== PORTS ===" && ss -tlnp | grep python
```

### Reading the `ss` output:

```
ss output format:
  LISTEN  0  5  0.0.0.0:46477  0.0.0.0:*  users:(("python",pid=97155,fd=21))
          │     │        │                              │
          │     │        └─ Port number                 └─ PID (match to ps output)
          │     └─ Bound to all IPv4 interfaces (CA server)
          └─ Backlog queue

  LISTEN  0  4  *:44725  *:*  users:(("python",pid=97155,fd=23))
                 │
                 └─ Bound to all interfaces IPv6 style (PVA server)

Rule of thumb:
  0.0.0.0:PORT  = CA listener (IPv4)
  *:PORT        = PVA listener (IPv6 wildcard)
```

### Matching PID → Process → Ports:

```bash
# Example investigation:
$ ps aux | grep "python examples" | grep -v grep
ernesto  77596  ...  python examples/math_model.py
ernesto  77744  ...  python examples/fft_model.py
ernesto  94294  ...  python examples/gaussian_sim.py
ernesto  97155  ...  python examples/gaussian_classical.py

$ ss -tlnp | grep "pid=97155"
LISTEN  0  5  0.0.0.0:46477  0.0.0.0:*  users:(("python",pid=97155,fd=21))  ← CA
LISTEN  0  4  *:44725         *:*        users:(("python",pid=97155,fd=23))  ← PVA

# Conclusion: gaussian_classical.py (PID 97155) → CA:46477, PVA:44725
```

### Full inventory when all examples are running:

```
HOST: lepton
═══════════════════════════════════════════════════════════════════════════
PID 77596 — python examples/math_model.py (tmux: math-model)
┌─────────────────────────────────────────────────────────────────────┐
│  Runner (SimpleMathModel)                                            │
│  CA port:  5064 (default — started first)                            │
│  PVA port: 5075 (default — started first)                            │
│  Serves: input_a, input_b, input_c, input_d, invert, my_enum,       │
│          desc, sum_output, model_info, RESET, SNAPSHOT               │
└─────────────────────────────────────────────────────────────────────┘

PID 77744 — python examples/fft_model.py (tmux: fft-model)
┌─────────────────────────────────────────────────────────────────────┐
│  SERVER A: SimpleSimulator (embedded, same process)                   │
│  CA port:  45061 (ephemeral)                                         │
│  PVA port: 41309 (ephemeral)                                         │
│  Serves: signal_a, signal_b, signal_c                                │
├─────────────────────────────────────────────────────────────────────┤
│  SERVER B: Runner (FFTModel)                                         │
│  CA port:  44485 (ephemeral)                                         │
│  PVA port: 37485 (ephemeral)                                         │
│  Subscribes to: signal_a, signal_b, signal_c                         │
│  Serves: fft_real, fft_imag, string_array, 2d_array,                 │
│          model_info, RESET, SNAPSHOT                                  │
└─────────────────────────────────────────────────────────────────────┘

PID 94294 — python examples/gaussian_sim.py (tmux: gauss-sim)
┌─────────────────────────────────────────────────────────────────────┐
│  p4p Server (direct, no Runner)                                      │
│  CA port:  NONE (PVA only)                                           │
│  PVA port: 42501 (ephemeral)                                         │
│  Serves: SIM:noisy_signal, SIM:clean_signal, SIM:x_axis,            │
│          SIM:mean, SIM:sigma, SIM:snr                                │
└─────────────────────────────────────────────────────────────────────┘

PID 97155 — python examples/gaussian_classical.py (tmux: gauss-denoise)
┌─────────────────────────────────────────────────────────────────────┐
│  Runner (GaussianDenoiserModel)                                      │
│  CA port:  46477 (ephemeral)                                         │
│  PVA port: 44725 (ephemeral)                                         │
│  Subscribes to: SIM:noisy_signal, SIM:x_axis                        │
│  Serves: est_mean, est_sigma, est_amplitude, denoised_signal,        │
│          fit_quality, model_info, RESET, SNAPSHOT                     │
└─────────────────────────────────────────────────────────────────────┘

TOTALS: 4 processes, 5 server instances, 4 CA listeners, 5 PVA listeners
```

### Why port numbers change every restart:

```
First server started  → gets default ports (5064 CA, 5075 PVA)
Second server started → 5064/5075 taken → OS assigns ephemeral (e.g., 45061/41309)
After restart         → different ephemeral ports assigned

This is normal. Clients find servers by PV NAME, not by port.
The EPICS discovery protocol (UDP search or TCP name server) handles it.
```

### Helper Script: `show_servers.sh`

Save to `~/controls/lume-pva/show_servers.sh`:

```bash
#!/bin/bash
# show_servers.sh — Show all running lume-pva EPICS servers with ports
# Usage: chmod +x show_servers.sh && ./show_servers.sh

echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "  RUNNING EPICS SERVERS"
echo "═══════════════════════════════════════════════════════════════════"
echo ""

# Find all relevant python processes
ps aux | grep -E "python (examples/|my_)" | grep -v grep | while read -r line; do
    pid=$(echo "$line" | awk '{print $2}')
    cmd=$(echo "$line" | awk '{for(i=11;i<=NF;i++) printf "%s ", $i; print ""}' | sed 's/ *$//')

    # Get CA ports (bound to 0.0.0.0)
    ca_ports=$(ss -tlnp 2>/dev/null | grep "pid=$pid," | grep "0.0.0.0:" | \
               awk '{print $4}' | awk -F: '{print $NF}' | sort -n | tr '\n' ',' | sed 's/,$//')

    # Get PVA ports (bound to *)
    pva_ports=$(ss -tlnp 2>/dev/null | grep "pid=$pid," | grep "\*:" | \
                awk '{print $4}' | awk -F: '{print $NF}' | sort -n | tr '\n' ',' | sed 's/,$//')

    echo "  PID: $pid"
    echo "  CMD: $cmd"
    echo "  CA:  ${ca_ports:-none}"
    echo "  PVA: ${pva_ports:-none}"
    echo "  ---"
done

echo ""
echo "  TMUX SESSIONS:"
tmux ls 2>/dev/null | sed 's/^/    /' || echo "    (none)"
echo ""
echo "═══════════════════════════════════════════════════════════════════"

# Summary counts
total_procs=$(ps aux | grep -E "python (examples/|my_)" | grep -v grep | wc -l)
total_ca=$(ss -tlnp 2>/dev/null | grep python | grep "0.0.0.0:" | wc -l)
total_pva=$(ss -tlnp 2>/dev/null | grep python | grep "\*:" | wc -l)
echo "  Summary: $total_procs processes, $total_ca CA listeners, $total_pva PVA listeners"
echo ""
```

Make executable and run:

```bash
chmod +x ~/controls/lume-pva/show_servers.sh
~/controls/lume-pva/show_servers.sh
```

---

## The Physics: Gaussian Curve Fitting Explained

### The Gaussian Function

```
                    ┌           ┐
                    │  (x - μ)² │
y(x) = A * exp │- ─────── │
                    │   2 * σ²  │
                    └           ┘

Where:
  A = amplitude (peak height)
  μ = mean (center position)
  σ = standard deviation (width)
```

### What `curve_fit` Does

Given noisy data points `(x[i], y[i])`, find the values of `A`, `μ`, `σ` that
minimize the sum of squared residuals:

```
minimize:  Σᵢ ( y[i] - A*exp(-0.5*((x[i]-μ)/σ)²) )²
```

This is **nonlinear least squares** — the Levenberg-Marquardt algorithm iteratively
adjusts parameters until the fit converges.

### Why R² ≈ 0.90 and not 1.0

```
R² = 1 - (SS_residual / SS_total)

SS_residual = Σ (y_measured - y_fit)²    ← noise leftover after fit
SS_total    = Σ (y_measured - mean(y))²  ← total variance

With SNR ≈ 13 dB:
  Signal power is ~20x noise power
  Noise accounts for ~5% of variance → R² ≈ 0.90-0.95
  
Higher SNR → R² closer to 1.0
Lower SNR  → R² drops toward 0.0
```

### Limitations of Classical Curve Fitting

| Limitation | When it fails |
|---|---|
| Assumes Gaussian shape | Real beams can be asymmetric or multi-peaked |
| Needs good initial guess | If noise >> signal, fit can converge to wrong minimum |
| Single model only | Can't handle mixture of Gaussians without modification |
| No uncertainty on outputs | curve_fit gives errors, but we don't propagate them (yet) |

These limitations motivate the ML denoiser (future example).


Here's the new section to add at the end of your runbook. I also found a few markdown syntax issues in your existing document that I'll note after the new section.

---

## New Section to Add (after the Physics section at the end of Part 6):

```markdown

---

## **Example 6: ML Gaussian Denoiser (`gaussian_train.py` + `gaussian_ml.py`)**

### **What it does:**

The same problem as the classical denoiser — estimate beam position and size from a noisy signal — but solved with a trained neural network instead of iterative curve fitting.

```
Classical approach (gaussian_classical.py):
  For EVERY new signal:
    Run Levenberg-Marquardt optimization (~50 iterations)
    Converge to best-fit parameters
    Time: ~2 ms per evaluation

ML approach (gaussian_ml.py):
  ONE-TIME training (gaussian_train.py):
    Generate 50,000 synthetic examples
    Train neural network to map signal → parameters
    Save trained weights to .pt file
    Time: ~30 seconds (once)

  For EVERY new signal:
    One forward pass through network (matrix multiplications)
    Time: ~0.4 ms per evaluation (5× faster)
```

### **Architecture:**

```
┌─────────────────────┐         ┌──────────────────────────────────┐
│  gaussian_train.py  │         │  gaussian_ml.py                   │
│  (run ONCE)         │         │  (run alongside simulator)        │
│                     │         │                                    │
│  1. Generate data   │         │  1. Load gaussian_model.pt         │
│  2. Train network   │─.pt───►│  2. Subscribe to SIM:noisy_signal  │
│  3. Save weights    │  file   │  3. Normalize input                │
│                     │         │  4. Forward pass (0.4 ms)          │
│  Output:            │         │  5. Denormalize output             │
│  gaussian_model.pt  │         │  6. Publish: ml_est_mean, etc.     │
└─────────────────────┘         └──────────────────────────────────┘
```

### **The Neural Network:**

```
Input Layer        Hidden Layers                    Output Layer
(noisy signal)     (learned feature extraction)     (beam params)

  256 values ──► [128 neurons] ──► [64] ──► [32] ──► 3 values
  (signal)        ReLU              ReLU     ReLU     (mean, sigma, amp)

Total parameters: 256×128 + 128×64 + 64×32 + 32×3 = ~43,000 weights
File size: ~173 KB
```

### **Why Normalization Matters:**

Neural networks work best when inputs and outputs are in the range [-1, 1].

```
TRAINING:
  signals_normalized = signals / signal_max      (÷ ~9.97)
  params_normalized = params / [5.0, 4.0, 8.0]  (per-parameter scaling)

INFERENCE (must match training exactly):
  input:  signal / 9.97 → network → raw_output × [5.0, 4.0, 8.0] → physical units
```

If you change `signal_max` or `param_scales` at training time, inference will produce garbage. These values are saved inside the `.pt` file.

---

### **Step 1: Train the Network (One-Time)**

```bash
tmux new-session -s gauss-train   # or use any terminal

conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src

python examples/gaussian_train.py
```

### **Expected output:**

```
Training on: cpu
Generating 50000 training samples...
  Signal normalization factor: 9.9702
  Training samples: 40000
  Validation samples: 10000

Training for 50 epochs...
 Epoch   Train Loss     Val Loss   Val MAE mean   Val MAE sigma
──────────────────────────────────────────────────────────────────
     0     0.065978     0.011935       0.3776 mm       0.4163 mm
     5     0.000521     0.000531       0.0823 mm       0.0783 mm
    10     0.000363     0.000367       0.0695 mm       0.0614 mm
    ...
    49     0.000205     0.000347       0.0791 mm       0.0607 mm

══════════════════════════════════════════════════════════════════
FINAL VALIDATION RESULTS (original scale):
  Mean estimation error:      0.0791 mm
  Sigma estimation error:     0.0607 mm
  Amplitude estimation error: 0.0953 a.u.

  For context:
    Beam wanders over ±5 mm → error is 1.6% of range
    Beam size 0.5–4.0 mm → error is 1.7% of range

✅ Model saved to: /home/ernesto/controls/lume-pva/lume-pva-src/examples/gaussian_model.pt
   File size: 173.1 KB
```

### **Interpreting training results:**

| **Metric** | **Meaning** | **Good values** |
| --- | --- | --- |
| Train Loss | How well the network fits training data | Decreasing each epoch |
| Val Loss | How well it generalizes to unseen data | Close to train loss (no overfitting) |
| Val MAE mean | Average error in beam position estimate | < 0.1 mm |
| Val MAE sigma | Average error in beam width estimate | < 0.1 mm |

**If val loss diverges from train loss:** The network is overfitting. Reduce epochs or add dropout.
**If both losses plateau early:** The network architecture may be too small. Add more neurons.

### **What's saved in `gaussian_model.pt`:**

| **Key** | **Value** | **Purpose** |
| --- | --- | --- |
| `model_state_dict` | Network weights (43K parameters) | The trained brain |
| `signal_max` | 9.9702 | Input normalization divisor |
| `param_scales` | [5.0, 4.0, 8.0] | Output denormalization multipliers |
| `num_points` | 256 | Expected input array size |
| `architecture` | "MLP_256_128_64_32_3" | Documentation string |

---

### **Step 2: Start the ML Denoiser**

**Prerequisites:** Simulator (`gaussian_sim.py`) must be running AND `gaussian_model.pt` must exist.

```bash
tmux new-session -s gauss-ml

conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src
python examples/gaussian_ml.py

# Detach: Ctrl+b then d
```

### **Expected startup output:**

```
Loaded trained model from /home/ernesto/controls/lume-pva/lume-pva-src/examples/gaussian_model.pt
  Running on: cpu
  Signal normalization: ÷9.9702
  Param scales: [5. 4. 8.]

════════════════════════════════════════════════════════════════
ML Denoiser running (prefix=none)
  Subscribes to: SIM:noisy_signal, SIM:x_axis
  Serves: ml_est_mean, ml_est_sigma, ml_est_amplitude
          ml_denoised, ml_fit_quality, ml_infer_time
  Try:  pvmonitor ml_est_mean ml_est_sigma ml_infer_time
════════════════════════════════════════════════════════════════
```

The port warnings (`Server unable to bind port 5075, falling back to...`) are normal — another server already has the default port.

---

### **Step 3: Monitor Results**

```bash
# ML denoiser outputs only
pvmonitor ml_est_mean ml_est_sigma ml_infer_time

# Side-by-side: Ground Truth vs Classical vs ML
pvmonitor SIM:mean est_mean ml_est_mean SIM:sigma est_sigma ml_est_sigma
```

### **Expected output (side-by-side comparison):**

```
SIM:mean     2026-07-31 23:31:32.135  -2.69576       ← ground truth
est_mean     2026-07-31 23:31:32.137  -2.62491       ← classical (curve_fit)
ml_est_mean  2026-07-31 23:31:32.138  -2.66482       ← ML (neural net)
SIM:sigma    2026-07-31 23:31:32.135  2.96129        ← ground truth
est_sigma    2026-07-31 23:31:32.137  2.91787        ← classical
ml_est_sigma 2026-07-31 23:31:32.138  2.87563        ← ML
```

### **PV Reference (ML Denoiser):**

| **PV Name** | **Type** | **R/W** | **Description** |
| --- | --- | --- | --- |
| `ml_est_mean` | float | ro | Estimated beam center from neural network (mm) |
| `ml_est_sigma` | float | ro | Estimated beam width from neural network (mm) |
| `ml_est_amplitude` | float | ro | Estimated peak height from neural network (a.u.) |
| `ml_denoised` | array (256) | ro | Reconstructed clean Gaussian from ML estimates |
| `ml_fit_quality` | float | ro | R² goodness of fit (0.0 to 1.0) |
| `ml_infer_time` | float | ro | Neural network forward pass time (seconds) |
| `RESET` | control | write | Reset model to defaults |
| `SNAPSHOT` | control | write | (unused in continuous mode) |
| `model_info` | struct | ro | Model metadata |

---

### **Performance Comparison: Classical vs ML**

| **Metric** | **Classical (curve_fit)** | **ML (neural network)** |
| --- | --- | --- |
| Time per evaluation | ~2 ms | ~0.4 ms |
| Mean accuracy | ~0.05 mm error | ~0.08 mm error |
| Sigma accuracy | ~0.04 mm error | ~0.06 mm error |
| Latency (sim → output) | ~3 ms | ~1 ms |
| Setup cost | Zero | Train once (~30s) |
| Dependencies | scipy | torch (~2 GB install) |
| Generalizes to non-Gaussian | ❌ No | ✅ Yes (if trained on it) |

**Key insight:** Classical wins on accuracy for perfectly Gaussian signals (it's mathematically optimal for this shape). ML wins on speed and generalizability. In a real system, you might use ML for real-time feedback (speed matters) and classical for offline analysis (accuracy matters).

---

### **Running the Full Three-Process Pipeline**

```bash
# Step 1: Simulator
tmux new-session -s gauss-sim
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src
python examples/gaussian_sim.py
# Ctrl+b, d

# Step 2: Classical denoiser
tmux new-session -s gauss-denoise
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src
python examples/gaussian_classical.py
# Ctrl+b, d

# Step 3: ML denoiser
tmux new-session -s gauss-ml
conda activate lume-pva
export EPICS_CA_ADDR_LIST=127.0.0.1
export EPICS_CA_AUTO_ADDR_LIST=NO
export EPICS_PVA_ADDR_LIST=127.0.0.1
export EPICS_PVA_AUTO_ADDR_LIST=NO
cd ~/controls/lume-pva/lume-pva-src
python examples/gaussian_ml.py
# Ctrl+b, d

# Step 4: Monitor all three
pvmonitor SIM:mean est_mean ml_est_mean SIM:sigma est_sigma ml_est_sigma
```

### **Full architecture when all three are running:**

```
HOST: lepton
═══════════════════════════════════════════════════════════════════════════

PID xxxxx — python examples/gaussian_sim.py (tmux: gauss-sim)
┌─────────────────────────────────────────────────────────────────────┐
│  p4p Server (direct, no Runner)                                      │
│  PVA port: ephemeral                                                 │
│  CA: none (PVA only)                                                 │
│  Serves: SIM:noisy_signal, SIM:clean_signal, SIM:x_axis,            │
│          SIM:mean, SIM:sigma, SIM:snr                                │
└──────────────────┬──────────────────────────────────┬───────────────┘
                   │                                   │
                   │ monitors SIM:noisy_signal         │ monitors SIM:noisy_signal
                   │ monitors SIM:x_axis               │ monitors SIM:x_axis
                   ▼                                   ▼
┌─────────────────────────────────────┐  ┌─────────────────────────────────────┐
│ PID xxxxx — gaussian_classical.py   │  │ PID xxxxx — gaussian_ml.py          │
│ (tmux: gauss-denoise)               │  │ (tmux: gauss-ml)                    │
│                                     │  │                                     │
│ Runner (GaussianDenoiserModel)      │  │ Runner (GaussianMLDenoiserModel)    │
│ Method: scipy curve_fit (~2 ms)     │  │ Method: PyTorch forward pass (~0.4ms│)
│ CA port: ephemeral                  │  │ CA port: ephemeral                  │
│ PVA port: ephemeral                 │  │ PVA port: ephemeral                 │
│                                     │  │                                     │
│ Serves:                             │  │ Serves:                             │
│   est_mean, est_sigma,              │  │   ml_est_mean, ml_est_sigma,        │
│   est_amplitude, denoised_signal,   │  │   ml_est_amplitude, ml_denoised,    │
│   fit_quality                       │  │   ml_fit_quality, ml_infer_time     │
└─────────────────────────────────────┘  └─────────────────────────────────────┘
                   │                                   │
                   └──────────────┬────────────────────┘
                                  ▼
                   ┌─────────────────────────────┐
                   │  CLIENT                      │
                   │  pvmonitor SIM:mean          │
                   │           est_mean           │
                   │           ml_est_mean        │
                   │                              │
                   │  Compares all three in       │
                   │  real-time                   │
                   └─────────────────────────────┘
```

---

### **Prerequisite: Installing PyTorch**

The ML denoiser requires PyTorch. Install it in the existing conda environment:

```bash
conda activate lume-pva
cd ~/controls/lume-pva/lume-pva-src
pip install -e ".[dev,torch]"
```

This installs the `[torch]` optional dependency group from `pyproject.toml`, which includes:
- `torch` — PyTorch library
- `lume-torch` — LUME's PyTorch variable types (TorchScalarVariable, TorchNDVariable)

### **Verify torch installation:**

```bash
python -c "import torch; print(f'PyTorch {torch.__version__}, CUDA: {torch.cuda.is_available()}')"
python -c "from lume_torch.variables import TorchNDVariable; print('lume-torch OK')"
```

**Note:** The ML denoiser runs fine on CPU. GPU (CUDA) makes training faster but is not required. Inference for this small network is fast on CPU (~0.4 ms).

---

### **Retraining the Model**

If you modify the simulator parameters (noise level, x-axis range, etc.) or want to improve accuracy:

```bash
# Delete old model
rm examples/gaussian_model.pt

# Edit gaussian_train.py if needed (change NUM_SAMPLES, EPOCHS, etc.)

# Retrain
python examples/gaussian_train.py

# Restart the ML denoiser (it loads the .pt file at startup)
tmux kill-session -t gauss-ml
tmux new-session -s gauss-ml
# ... (same startup commands as above)
```

### **Training tips:**

| **Want this** | **Change this** |
| --- | --- |
| Better accuracy | Increase `NUM_SAMPLES` (more training data) |
| Faster training | Decrease `EPOCHS` (may reduce accuracy) |
| Handle wider beam range | Adjust `np.random.uniform` bounds in `generate_training_data()` |
| Handle different noise levels | Randomize `noise_level` in training data generation |
| Faster inference | Reduce hidden layer sizes (128→64→32→16) |

---

### **Key Architectural Insight**

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                      │
│  The MODEL INTERFACE is IDENTICAL between classical and ML.          │
│                                                                      │
│  Both implement:  __init__, supported_variables, _get, _set, reset   │
│  Both use:        Runner.generate_config() + Runner.run()            │
│  Both subscribe:  to the same simulator PVs                          │
│  Both publish:    their own output PVs                               │
│                                                                      │
│  THE ONLY DIFFERENCE IS WHAT'S INSIDE _set():                        │
│    Classical: scipy.optimize.curve_fit (iterative optimization)       │
│    ML:        torch forward pass (matrix multiplication chain)        │
│                                                                      │
│  This is the WHOLE POINT of lume-pva:                                │
│    Swap algorithms without changing the infrastructure.              │
│    Clients don't know or care HOW the answer was computed.           │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

### **Troubleshooting (ML-Specific)**

| **Problem** | **Fix** |
| --- | --- |
| `FileNotFoundError: gaussian_model.pt` | Run `python examples/gaussian_train.py` first |
| `ModuleNotFoundError: torch` | Run `pip install -e ".[dev,torch]"` |
| `ModuleNotFoundError: lume_torch` | Same fix — `[torch]` extra installs lume-torch |
| Training loss doesn't decrease | Check learning rate, try `LEARNING_RATE = 0.0001` |
| ML estimates are consistently biased | Retrain with more samples or check normalization |
| `ml_infer_time` > 10ms | Something is wrong — check for CPU throttling |
| CUDA out of memory (during training) | Reduce `BATCH_SIZE` or `NUM_SAMPLES` |
| Inference works but values are wrong | `.pt` file was trained with different simulator params — retrain |
```


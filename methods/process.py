#!/usr/bin/python3
# coding=utf-8

#   Copyright 2025 EPAM Systems
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

""" Process management for syngen subprocess execution """

import re
import os
import sys
import time
import subprocess
import select
import signal
import threading

from pylon.core.tools import log  # pylint: disable=E0611,E0401,W0611
from pylon.core.tools import web  # pylint: disable=E0611,E0401,W0611


# Regex to strip ANSI escape codes
ANSI_ESCAPE_PATTERN = re.compile(r'\x1b\[[0-9;]*m')

# Patterns for filtering noisy log lines
PROGRESS_BAR_PATTERN = re.compile(r'^\d+it \[[\d:]+,.*it/s\]$')  # tqdm progress bars
EPOCH_PATTERN = re.compile(r'^epoch:\s*(\d+),\s*total loss:\s*([\d.]+)')  # epoch summary lines


class ThinkingThrottler:
    """
    Throttles thinking step emissions to prevent UI flickering.
    Batches logs within a time window and only emits meaningful summaries.
    
    Simple approach: accumulate lines and only emit when batch_interval has passed.
    No timers - relies on continuous add_line calls during training.
    """
    
    def __init__(self, emit_callback, checkpoint_callback=None, batch_interval_ms=1000):
        self.emit_callback = emit_callback
        self.checkpoint_callback = checkpoint_callback  # Called before each emit
        self.batch_interval = batch_interval_ms / 1000.0  # Convert to seconds
        self.pending_lines = []
        self.last_emit_time = 0
        self.last_epoch = None
        self.last_loss = None
    
    def add_line(self, line):
        """Add a line to the batch. Emits if enough time has passed."""
        # Check if this is a progress bar line (very noisy, skip entirely)
        if PROGRESS_BAR_PATTERN.match(line):
            return
        
        # Check if this is an epoch line - extract and store epoch/loss
        epoch_match = EPOCH_PATTERN.match(line)
        if epoch_match:
            self.last_epoch = int(epoch_match.group(1))
            self.last_loss = float(epoch_match.group(2))
        else:
            # For other lines, add to pending
            self.pending_lines.append(line)
        
        # Check if we should emit now
        current_time = time.time()
        if current_time - self.last_emit_time >= self.batch_interval:
            self._do_emit()
    
    def _do_emit(self):
        """Actually emit the batched content."""
        self.last_emit_time = time.time()
        
        messages = []
        
        # If we have epoch info, emit a clean summary
        if self.last_epoch is not None:
            messages.append(f"Training epoch {self.last_epoch}, loss: {self.last_loss:.4f}")
            self.last_epoch = None
            self.last_loss = None
        
        # Add any other pending lines (non-epoch, non-progress)
        for line in self.pending_lines:
            # Skip very short or empty lines
            if len(line.strip()) > 3:
                messages.append(line)
        
        self.pending_lines = []
        
        # Emit combined message
        if messages:
            # Call checkpoint before emitting (required by pylon framework)
            if self.checkpoint_callback:
                try:
                    self.checkpoint_callback()
                except Exception:
                    pass  # Stop was requested, but we still emit
            
            # Combine all messages into a single thinking step
            combined = "\n".join(messages)
            try:
                self.emit_callback(combined)
            except Exception:
                pass  # Emit failed, continue anyway
    
    def flush(self):
        """Flush any remaining pending content."""
        self._do_emit()


def strip_ansi_codes(text):
    """Remove ANSI escape codes from text"""
    return ANSI_ESCAPE_PATTERN.sub('', text)


def clean_log_line(line):
    """Clean a log line for display - strip ANSI codes and extract message"""
    # Strip ANSI codes
    clean = strip_ansi_codes(line)
    # Remove timestamp prefix if present (format: 2025-12-02 08:22:05.314)
    # Pattern: YYYY-MM-DD HH:MM:SS.mmm | LEVEL | module:function:line - message
    match = re.match(
        r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s*\|\s*\w+\s*\|\s*[\w.:]+\s*-\s*(.+)$',
        clean
    )
    if match:
        return match.group(1).strip()
    return clean.strip()


class SyngenSubprocessRunner:
    """
    Runs syngen commands in a subprocess with real-time output streaming.
    
    This is the library-based equivalent of DockerSocketClient from the docker branch.
    Instead of running syngen via Docker API, it spawns a subprocess that imports
    and runs syngen directly.
    """
    
    def __init__(self, work_path, python_executable=None):
        """
        Initialize the runner.
        
        Args:
            work_path: Working directory for syngen operations
            python_executable: Path to Python executable (defaults to sys.executable)
        """
        self.work_path = work_path
        self.python_executable = python_executable or sys.executable
        self.process = None
        self.stop_requested = False
    
    def run_train(self, table_name, source_file, epochs, batch_size, drop_null=False,
                  output_callback=None, checkpoint_callback=None):
        """
        Run syngen training in a subprocess.
        
        Args:
            table_name: Model name
            source_file: Path to training data file
            epochs: Number of training epochs
            batch_size: Batch size for training
            drop_null: Whether to drop null values
            output_callback: Callback for output lines
            checkpoint_callback: Callback for stop checking
            
        Returns:
            Tuple of (model_path, actual_model_dir_name)
        """
        # Build the Python script to execute
        script = self._build_train_script(
            table_name=table_name,
            source_file=source_file,
            epochs=epochs,
            batch_size=batch_size,
            drop_null=drop_null
        )
        
        return self._run_subprocess(
            script=script,
            output_callback=output_callback,
            checkpoint_callback=checkpoint_callback,
            operation="training"
        )
    
    def run_infer(self, table_name, size, batch_size, run_parallel=False, random_seed=None,
                  output_callback=None, checkpoint_callback=None):
        """
        Run syngen inference in a subprocess.
        
        Args:
            table_name: Model name (should match directory name, typically hyphenated)
            size: Number of rows to generate
            batch_size: Batch size for inference
            run_parallel: Whether to run in parallel
            random_seed: Optional random seed
            output_callback: Callback for output lines
            checkpoint_callback: Callback for stop checking
            
        Returns:
            Path to output directory
        """
        script = self._build_infer_script(
            table_name=table_name,
            size=size,
            batch_size=batch_size,
            run_parallel=run_parallel,
            random_seed=random_seed
        )
        
        return self._run_subprocess(
            script=script,
            output_callback=output_callback,
            checkpoint_callback=checkpoint_callback,
            operation="inference"
        )
    
    def _build_train_script(self, table_name, source_file, epochs, batch_size, drop_null):
        """Build Python script for training subprocess"""
        # Get sys.path from parent process to include plugin requirements
        parent_sys_path = repr(sys.path)
        
        return f'''
import sys
import os

# Add parent process sys.path to access plugin dependencies (syngen, loguru, etc.)
# This includes /data/requirements/syngen_plugin/lib/python3.x/site-packages
for p in {parent_sys_path}:
    if p not in sys.path:
        sys.path.insert(0, p)

# Configure loguru to output to stderr for capture
from loguru import logger
logger.remove()
logger.add(
    sys.stderr,
    level="DEBUG",
    format="{{time:YYYY-MM-DD HH:mm:ss.SSS}} | {{level: <8}} | {{name}}:{{function}}:{{line}} - {{message}}"
)

# Change to work directory
os.chdir({repr(self.work_path)})

# Import Worker
from syngen.ml.worker import Worker

# Settings
settings = {{
    "source": {repr(source_file)},
    "epochs": {epochs},
    "batch_size": {batch_size},
    "drop_null": {drop_null},
    "row_limit": None,
    "reports": ["none"],
    "print_report": False,
    "generate_data": False,
}}

# Create Worker
worker = Worker(
    table_name={repr(table_name)},
    metadata_path=None,
    settings=settings,
    log_level="INFO",
    type_of_process="train",
    encryption_settings={{}},
    train_stages=["PREPROCESS", "TRAIN", "POSTPROCESS"],
    infer_stages=[]
)

# Run training
worker.launch_train()

# Signal completion
print("SYNGEN_TRAINING_COMPLETE", file=sys.stderr, flush=True)
'''
    
    def _build_infer_script(self, table_name, size, batch_size, run_parallel, random_seed):
        """Build Python script for inference subprocess"""
        output_filename = f"merged_infer_{table_name}.csv"
        output_path = os.path.join(self.work_path, "output", output_filename)
        
        random_seed_line = f'"random_seed": {random_seed},' if random_seed is not None else ''
        
        # Get sys.path from parent process to include plugin requirements
        parent_sys_path = repr(sys.path)
        
        return f'''
import sys
import os

# Add parent process sys.path to access plugin dependencies (syngen, loguru, etc.)
# This includes /data/requirements/syngen_plugin/lib/python3.x/site-packages
for p in {parent_sys_path}:
    if p not in sys.path:
        sys.path.insert(0, p)

# Configure loguru to output to stderr for capture
from loguru import logger
logger.remove()
logger.add(
    sys.stderr,
    level="DEBUG",
    format="{{time:YYYY-MM-DD HH:mm:ss.SSS}} | {{level: <8}} | {{name}}:{{function}}:{{line}} - {{message}}"
)

# Change to work directory
os.chdir({repr(self.work_path)})

# Ensure output directory exists
os.makedirs({repr(os.path.dirname(output_path))}, exist_ok=True)

# Import Worker
from syngen.ml.worker import Worker

# Settings
settings = {{
    "size": {size},
    "batch_size": {batch_size},
    "run_parallel": {run_parallel},
    "destination": {repr(output_path)},
    "reports": ["none"],
    {random_seed_line}
}}

# Create Worker
worker = Worker(
    table_name={repr(table_name)},
    metadata_path=None,
    settings=settings,
    log_level="INFO",
    type_of_process="infer",
    encryption_settings={{}}
)

# Run inference
worker.launch_infer()

# Signal completion with output path
print("SYNGEN_INFER_COMPLETE:" + {repr(os.path.dirname(output_path))}, file=sys.stderr, flush=True)
'''
    
    def _run_subprocess(self, script, output_callback, checkpoint_callback, operation):
        """
        Run a subprocess with the given script and stream output.
        
        Args:
            script: Python script to execute
            output_callback: Callback for each output line
            checkpoint_callback: Callback for stop checking
            operation: Description of operation (for logging)
            
        Returns:
            Result from subprocess (varies by operation)
        """
        self.stop_requested = False
        result = None
        
        try:
            # Start subprocess
            self.process = subprocess.Popen(
                [self.python_executable, "-u", "-c", script],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,  # Merge stderr into stdout
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
                cwd=self.work_path,
                text=True,
                bufsize=1  # Line buffered
            )
            
            log.info("Started syngen %s subprocess (PID: %d)", operation, self.process.pid)
            
            # Stream output
            while True:
                # Check for stop request
                if checkpoint_callback:
                    try:
                        checkpoint_callback()
                    except Exception:
                        # Stop requested
                        log.info("Stop requested, terminating subprocess...")
                        self._terminate_process()
                        raise
                
                # Check if process is still running
                if self.process.poll() is not None:
                    # Process finished, drain remaining output
                    for line in self.process.stdout:
                        line = line.strip()
                        if line:
                            # Log to docker/pylon logs
                            print(line, file=sys.__stdout__, flush=True)
                            
                            # Check for completion signals
                            if line.startswith("SYNGEN_TRAINING_COMPLETE"):
                                result = self._find_model_path()
                            elif line.startswith("SYNGEN_INFER_COMPLETE:"):
                                result = line.split(":", 1)[1]
                            
                            # Send to callback
                            if output_callback:
                                clean_line = clean_log_line(line)
                                if clean_line:
                                    output_callback(clean_line)
                    break
                
                # Read available output with timeout
                try:
                    # Use select for non-blocking read on Unix
                    import select as sel
                    ready, _, _ = sel.select([self.process.stdout], [], [], 0.5)
                    
                    if ready:
                        line = self.process.stdout.readline()
                        if line:
                            line = line.strip()
                            if line:
                                # Log to docker/pylon logs
                                print(line, file=sys.__stdout__, flush=True)
                                
                                # Check for completion signals
                                if line.startswith("SYNGEN_TRAINING_COMPLETE"):
                                    result = self._find_model_path()
                                elif line.startswith("SYNGEN_INFER_COMPLETE:"):
                                    result = line.split(":", 1)[1]
                                
                                # Send to callback
                                if output_callback:
                                    clean_line = clean_log_line(line)
                                    if clean_line:
                                        output_callback(clean_line)
                except Exception as e:
                    # On Windows or if select fails, use simple blocking read
                    line = self.process.stdout.readline()
                    if line:
                        line = line.strip()
                        if line:
                            print(line, file=sys.__stdout__, flush=True)
                            if line.startswith("SYNGEN_TRAINING_COMPLETE"):
                                result = self._find_model_path()
                            elif line.startswith("SYNGEN_INFER_COMPLETE:"):
                                result = line.split(":", 1)[1]
                            if output_callback:
                                clean_line = clean_log_line(line)
                                if clean_line:
                                    output_callback(clean_line)
            
            # Check exit code
            exit_code = self.process.returncode
            if exit_code != 0:
                raise RuntimeError(f"Syngen {operation} subprocess failed with exit code {exit_code}")
            
            log.info("Syngen %s subprocess completed successfully", operation)
            return result
            
        finally:
            self.process = None
    
    def _find_model_path(self):
        """Find the model path after training completes"""
        # syngen uses slugify which converts underscores to hyphens
        # Look in model_artifacts/resources/ for any model directory
        resources_path = os.path.join(self.work_path, "model_artifacts", "resources")
        
        if not os.path.exists(resources_path):
            # Try alternative paths
            alt_paths = [
                os.path.join(self.work_path, "resources"),
            ]
            for alt in alt_paths:
                if os.path.exists(alt):
                    resources_path = alt
                    break
        
        if os.path.exists(resources_path):
            dirs = os.listdir(resources_path)
            if dirs:
                # Return first model directory found
                model_dir = dirs[0]
                model_path = os.path.join(resources_path, model_dir)
                return (model_path, model_dir)
        
        # Fallback: search recursively
        for root, dirs, files in os.walk(self.work_path):
            for d in dirs:
                if "train_message.success" in os.listdir(os.path.join(root, d)):
                    model_path = os.path.join(root, d)
                    return (model_path, d)
        
        raise RuntimeError(f"Model not found in {self.work_path}")
    
    def _terminate_process(self):
        """Terminate the subprocess gracefully"""
        if self.process and self.process.poll() is None:
            self.stop_requested = True
            
            # Try graceful termination first
            self.process.terminate()
            
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill
                self.process.kill()
                self.process.wait()
            
            log.info("Subprocess terminated")


class Method:  # pylint: disable=E1101,R0903,W0201
    """
        Method Resource

        self is pointing to current Module instance

        web.method decorator takes zero or one argument: method name
        Note: web.method decorator must be the last decorator (at top)
    """

    @web.method()
    def run_syngen_train(self, work_path, table_name, source_file, epochs, batch_size, drop_null=False):
        """
        Run syngen training in a subprocess with real-time output streaming.
        
        This is the library-based equivalent of run_syngen_command from docker branch.
        
        Args:
            work_path: Working directory for syngen
            table_name: Model name
            source_file: Path to training data file
            epochs: Number of epochs
            batch_size: Batch size
            drop_null: Whether to drop nulls
            
        Returns:
            Tuple of (model_path, actual_model_dir_name)
        """
        # Create throttler for thinking steps
        throttler = ThinkingThrottler(
            emit_callback=lambda msg: self.invocation_thinking(msg),
            checkpoint_callback=lambda: self.invocation_stop_checkpoint(),
            batch_interval_ms=1000
        )
        
        def on_output_line(line):
            """Process each output line"""
            log.info("[syngen] %s", line)
            throttler.add_line(line)
        
        # Run subprocess
        runner = SyngenSubprocessRunner(work_path)
        
        try:
            result = runner.run_train(
                table_name=table_name,
                source_file=source_file,
                epochs=epochs,
                batch_size=batch_size,
                drop_null=drop_null,
                output_callback=on_output_line,
                checkpoint_callback=lambda: self.invocation_stop_checkpoint()
            )
        finally:
            # Flush remaining thinking updates
            throttler.flush()
        
        return result

    @web.method()
    def run_syngen_infer(self, work_path, table_name, size, batch_size, run_parallel=False, random_seed=None):
        """
        Run syngen inference in a subprocess with real-time output streaming.
        
        Args:
            work_path: Working directory for syngen (must contain model_artifacts/resources/)
            table_name: Model name (should match directory name, typically hyphenated)
            size: Number of rows to generate
            batch_size: Batch size
            run_parallel: Whether to run in parallel
            random_seed: Optional random seed
            
        Returns:
            Path to output directory containing generated CSV
        """
        # Create throttler for thinking steps
        throttler = ThinkingThrottler(
            emit_callback=lambda msg: self.invocation_thinking(msg),
            checkpoint_callback=lambda: self.invocation_stop_checkpoint(),
            batch_interval_ms=1000
        )
        
        def on_output_line(line):
            """Process each output line"""
            log.info("[syngen] %s", line)
            throttler.add_line(line)
        
        # Run subprocess
        runner = SyngenSubprocessRunner(work_path)
        
        try:
            result = runner.run_infer(
                table_name=table_name,
                size=size,
                batch_size=batch_size,
                run_parallel=run_parallel,
                random_seed=random_seed,
                output_callback=on_output_line,
                checkpoint_callback=lambda: self.invocation_stop_checkpoint()
            )
        finally:
            # Flush remaining thinking updates
            throttler.flush()
        
        return result

    @web.method()
    def parse_syngen_line(self, line):
        """
        Parse syngen log output and emit thinking updates.
        
        Syngen uses loguru with format like:
        2024-01-01 12:00:00.000 | INFO     | syngen.module:function:123 - Message
        """
        # Pattern to match loguru format
        loguru_pattern = r'^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s+\|\s+\w+\s+\|\s+[\w\._:]+\s+-\s+(.+)$'
        
        match = re.match(loguru_pattern, line)
        if match:
            message = match.group(1)
            self.invocation_thinking(message)
        else:
            # If it doesn't match loguru format, check if it's a simple info line
            if line and not line.startswith(' ') and len(line) > 5:
                # Filter out noisy lines
                skip_patterns = [
                    r'^Traceback',
                    r'^\s+File\s+',
                    r'^\s+raise\s+',
                    r'^$',
                ]
                for pattern in skip_patterns:
                    if re.match(pattern, line):
                        return
                
                self.invocation_thinking(line)

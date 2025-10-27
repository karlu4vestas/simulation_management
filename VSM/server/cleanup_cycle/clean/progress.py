"""Handles periodic progress reporting to the console."""

import time
import sys
import config
import shared_state

terminal_width=None
def get_terminal_width() -> int:
    global terminal_width
    if terminal_width is None:
        # Ensure output doesn't wrap unnecessarily / provide space for updates
        terminal_width = 80 # Assume a default, or try getting dynamically
        try:
            import shutil
            terminal_width = shutil.get_terminal_size().columns
        except (ImportError, OSError):
             pass # Use default if cannot get terminal size
    return terminal_width

def progress_reporter(start_time: float, total_files: int | None):
    while not shared_state.shutdown_requested.wait(config.PROGRESS_INTERVAL_SECONDS):
        elapsed_time = time.monotonic() - start_time
        s_count, f_count = shared_state.get_counts() # Get counts thread-safely

        processed_count = s_count + f_count
        progress_percent_str = "N/A"
        if total_files and total_files > 0:
             progress_percent = (processed_count / total_files * 100)
             progress_percent_str = f"{progress_percent:.2f}%"

        rate = processed_count / elapsed_time if elapsed_time > 0 else 0


        status_line = (f"Progress: Elapsed={elapsed_time:.1f}s | "
                       f"Processed={processed_count}" + (f"/{total_files}" if total_files else "") +
                       f" ({progress_percent_str})" +
                       f" | OK={s_count} | Fail={f_count} | Rate={rate:.1f} files/s")

        # Pad with spaces to clear previous line content if shorter
        padded_line = status_line.ljust(get_terminal_width() -1) # -1 for cursor pos

        print(padded_line, end='\r', file=sys.stdout, flush=True) # Use stdout, flush needed with \r

    # Final progress print before exiting
    elapsed_time = time.monotonic() - start_time
    s_count, f_count = shared_state.get_counts()
    processed_count = s_count + f_count
    rate = processed_count / elapsed_time if elapsed_time > 0 else 0
    final_status = (f"Final Progress: Elapsed={elapsed_time:.1f}s | Processed={processed_count} | "
                    f"OK={s_count} | Fail={f_count} | Rate={rate:.1f} files/s")
    print(final_status.ljust(get_terminal_width()), file=sys.stdout) # Print final stats clearly
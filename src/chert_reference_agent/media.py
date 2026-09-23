"""Pure bounded synthetic fixtures; no device input or retained caller media."""

from __future__ import annotations

import math
import struct

SAMPLE_RATE = 48_000
SAMPLES_PER_CHUNK = 960
WIDTH, HEIGHT = 320, 180
FPS = 10


def audio_chunk(sequence: int) -> bytes:
    """20ms mono PCM16 with a deterministic pulsed 440Hz tone at <=1/8 scale."""
    if sequence < 0:
        raise ValueError("negative sequence")
    # Alternate one second tone / half second quiet, preserving phase at boundaries.
    amplitude = 4096 if sequence % 75 < 50 else 0
    start = sequence * SAMPLES_PER_CHUNK
    return struct.pack(
        "<960h",
        *(
            round(amplitude * math.sin(2 * math.pi * 440 * ((start + i) % SAMPLE_RATE) / SAMPLE_RATE))
            for i in range(SAMPLES_PER_CHUNK)
        ),
    )


def video_frame(sequence: int) -> bytes:
    """RGB color bars with a binary frame counter strip; exactly 320x180 pixels."""
    if sequence < 0:
        raise ValueError("negative sequence")
    colors = (
        (230, 230, 230),
        (230, 230, 0),
        (0, 230, 230),
        (0, 230, 0),
        (230, 0, 230),
        (230, 0, 0),
        (0, 0, 230),
        (20, 20, 20),
    )
    row = b"".join(bytes(colors[(x // 40 + sequence // FPS) % 8]) for x in range(WIDTH))
    counter = b"".join(
        bytes((255, 255, 255) if (sequence >> (x // 10)) & 1 else (0, 0, 0)) for x in range(WIDTH)
    )
    return row * (HEIGHT - 20) + counter * 20

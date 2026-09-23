import hashlib
import struct

from chert_reference_agent.media import audio_chunk, video_frame


def test_audio_is_bounded_deterministic_mono_20ms_and_changes():
    chunks = [audio_chunk(i) for i in range(8)]
    assert all(len(chunk) == 960 * 2 for chunk in chunks)
    samples = struct.unpack("<960h", chunks[0])
    assert 0 < max(samples) <= 4096
    assert min(samples) >= -4096
    assert chunks == [audio_chunk(i) for i in range(8)]
    assert len(set(chunks)) > 1


def test_video_is_deterministic_rgb_changes_and_bounded():
    first, second = video_frame(0), video_frame(1)
    assert len(first) == 320 * 180 * 3
    assert first != second
    assert hashlib.sha256(first).digest() == hashlib.sha256(video_frame(0)).digest()


def test_fixture_rejects_negative_sequence():
    import pytest

    with pytest.raises(ValueError):
        audio_chunk(-1)
    with pytest.raises(ValueError):
        video_frame(-1)

from unittest.mock import Mock

import pytest
from pydantic_ai import ModelRetry
from pydantic_ai.messages import ModelResponse, TextPart
from pydantic_ai.models.function import FunctionModel

from podcast.summary import (
    EpisodeSummary,
    agent,
    render_summary,
    summarize,
    validate_summary,
)
from podcast.transcribe import Segment, Transcript, timestamp


@pytest.fixture
def transcript():
    return Transcript(
        language="en",
        segments=[
            Segment(
                start=0,
                end=31,
                text="We tried a smaller model and cut the cost in half.",
            ),
            Segment(
                start=31,
                end=65,
                text="But it failed our harder tasks, so we kept the larger model for those.",
            ),
        ],
    )


@pytest.fixture
def brief():
    return EpisodeSummary(
        title="Where smaller models stop saving money",
        synopsis="The hosts compare cheaper inference with the cost of failures.",
        takeaways=["Evaluate on difficult tasks before switching models."],
        topics=[
            dict(
                start_segment=0,
                end_segment=1,
                title="Cost versus reliability",
                summary="A cheaper model works only for easier tasks.",
            )
        ],
        clips=[
            dict(
                start_segment=0,
                end_segment=1,
                title="The hidden cost",
                summary="A complete example with a clear reversal.",
            )
        ],
        producer_notes=[dict(segment=0, note="Verify the claimed cost reduction.")],
    )


def test_agent_round_trip_and_timestamps(transcript, brief):
    """Exercise real PydanticAI validation and render timecodes from the transcript."""
    model = FunctionModel(
        lambda messages, info: ModelResponse(parts=[TextPart(brief.model_dump_json())])
    )
    with agent.override(model=model):
        result = summarize(transcript)
    assert result == brief
    rendered = render_summary(result, transcript)
    assert "00:00:00–00:01:05" in rendered
    assert "**00:00:00**" in rendered
    assert timestamp(3661.9) == "01:01:01"


@pytest.mark.parametrize("start,end", [(1, 0), (0, 2)])
def test_invalid_ranges_are_retried(transcript, brief, start, end):
    """A valid schema must still reference real, ordered source segments."""
    brief.clips[0].start_segment = start
    brief.clips[0].end_segment = end
    with pytest.raises(ModelRetry):
        validate_summary(Mock(deps=transcript), brief)


def test_invalid_note_is_retried(transcript, brief):
    """Producer notes cannot cite a segment the producer cannot find."""
    brief.producer_notes[0].segment = 2
    with pytest.raises(ModelRetry):
        validate_summary(Mock(deps=transcript), brief)


def test_overlapping_topics_are_retried(transcript, brief):
    """The rundown must progress through the episode without duplicate coverage."""
    brief.topics.append(brief.topics[0])
    with pytest.raises(ModelRetry):
        validate_summary(Mock(deps=transcript), brief)


def test_clip_cannot_end_in_partial_sentence(transcript, brief):
    """Whisper segments can end mid-thought; an editor needs a complete clip ending."""
    transcript.segments[1].text = "A complete sentence. Followed by an unfinished"
    with pytest.raises(ModelRetry, match="mid-sentence"):
        validate_summary(Mock(deps=transcript), brief)

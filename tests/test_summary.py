from unittest.mock import Mock

import pytest
from pydantic_ai import ModelRetry
from pydantic_ai.messages import ModelResponse, TextPart
from pydantic_ai.models.function import FunctionModel

from podcast.summary import (
    EpisodeSummary,
    SpeakerName,
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
        background="An AI model is software trained on examples. Inference means using it to produce an answer.",
        speakers=[],
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
    assert (
        rendered.index("## Background")
        < rendered.index(brief.background)
        < rendered.index("## What we talked about")
        < rendered.index(brief.synopsis)
        < rendered.index("## Key takeaways")
    )
    assert "00:00:00–00:01:05" in rendered
    assert "Producer notes" not in rendered
    assert "producer_notes" not in result.model_dump()
    assert timestamp(3661.9) == "01:01:01"


@pytest.mark.parametrize("start,end", [(1, 0), (0, 2)])
def test_invalid_ranges_are_retried(transcript, brief, start, end):
    """A valid schema must still reference real, ordered source segments."""
    brief.clips[0].start_segment = start
    brief.clips[0].end_segment = end
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


def test_named_speaker_must_cite_their_own_introduction(transcript, brief):
    transcript.segments[0].text = "I'm Alex."
    transcript.segments[0].speaker = "speaker_1"
    transcript.segments[1].speaker = "speaker_2"
    person = SpeakerName(speaker="speaker_1", name="Alex", introduction_segment=0)
    brief.speakers = [person]
    assert validate_summary(Mock(deps=transcript), brief) == brief
    assert "Alex (speaker_1)" in render_summary(brief, transcript)
    person.introduction_segment = 1
    with pytest.raises(ModelRetry, match="own labeled introduction"):
        validate_summary(Mock(deps=transcript), brief)


@pytest.mark.parametrize("invalid", ["unknown", "out_of_range", "duplicate"])
def test_invalid_speaker_mappings_are_retried(transcript, brief, invalid):
    transcript.segments[0].speaker = "speaker_1"
    person = SpeakerName(speaker="speaker_1", name="Alex", introduction_segment=0)
    brief.speakers = [person]
    if invalid == "unknown":
        person.speaker = "speaker_3"
    elif invalid == "out_of_range":
        person.introduction_segment = len(transcript.segments)
    else:
        brief.speakers.append(person)
    with pytest.raises(ModelRetry, match="own labeled introduction"):
        validate_summary(Mock(deps=transcript), brief)

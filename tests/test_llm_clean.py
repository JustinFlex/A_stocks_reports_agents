from astock_report.workflows.nodes.llm_clean import clean_llm_output


def test_clean_llm_output_strips_thinking_and_quotes():
    raw = "*Thinking...*\n\n> step 1\n> step 2\n\nMain body text.\n\nMore."
    cleaned = clean_llm_output(raw)
    assert "Thinking" not in cleaned
    assert "step 1" not in cleaned
    assert cleaned.startswith("Main body text.")

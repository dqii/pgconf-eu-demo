import gradio as gr
from dotenv import load_dotenv
from ask_question import ask_question

load_dotenv()


def chat_with_context(provider, repo, question, context_types):
    return ask_question(provider, repo, question, context_types, return_prompt=True)


def chat_without_context(provider, repo, question):
    return ask_question(provider, repo, question, [])


# Define the Gradio interface.
with gr.Blocks() as demo:
    gr.Markdown("# Chat Interface with Ubicloud and OpenAI")

    # First row: Repository selection
    repo = gr.Radio(["pg_cron", "citus", "ubicloud", "postgres"],
                    label="Select Repository", value="pg_cron", interactive=True)

    # Second row: Question input
    question = gr.Textbox(
        label="Question", placeholder="Enter your question")

    # Third row: Context types selection
    context_types = gr.CheckboxGroup(
        ["files", "folders", "commits"], label="Select Context Types", value=["files"], interactive=True)

    # Fourth row: Create a grid layout for the response panels.
    with gr.Row():
        # No context responses column.
        with gr.Column():
            gr.Markdown("### OpenAI Response (No Context)")
            output_openai_no_context = gr.Markdown(
                label="OpenAI Response (No Context)")
            gr.Markdown("---")

        with gr.Column():
            gr.Markdown("### Llama Response (No Context)")
            output_ubicloud_no_context = gr.Markdown(
                label="Llama Response (No Context)")
            gr.Markdown("---")

    with gr.Row():
        # With context responses column.
        with gr.Column():
            gr.Markdown("### OpenAI Response (With Context)")
            output_openai_with_context = gr.Markdown(
                label="OpenAI Response (With Context)")
            gr.Markdown("---")

        with gr.Column():
            gr.Markdown("### Llama Response (With Context)")
            output_ubicloud_with_context = gr.Markdown(
                label="Llama Response (With Context)")
            gr.Markdown("---")

    with gr.Row():
        # With context prompt column.
        with gr.Column():
            gr.Markdown("### OpenAI Prompt (With Context)")
            output_openai_with_context_prompt = gr.Markdown(
                label="OpenAI Prompt (With Context)")
            gr.Markdown("---")

        with gr.Column():
            gr.Markdown("### Llama Prompt (With Context)")
            output_ubicloud_with_context_prompt = gr.Markdown(
                label="Llama Prompt (With Context)")
            gr.Markdown("---")

    # Submit button to call the respective functions
    submit_btn = gr.Button("Ask")

    # Function calls for each of the output panels
    submit_btn.click(
        fn=lambda repo, question: chat_without_context(
            "openai", repo, question),
        inputs=[repo, question],
        outputs=output_openai_no_context
    )
    submit_btn.click(
        fn=lambda repo, question: chat_without_context(
            "ubicloud", repo, question),
        inputs=[repo, question],
        outputs=output_ubicloud_no_context
    )
    submit_btn.click(
        fn=lambda repo, question, context_types: chat_with_context(
            "openai", repo, question, context_types),
        inputs=[repo, question, context_types],
        outputs=[output_openai_with_context, output_openai_with_context_prompt]
    )
    submit_btn.click(
        fn=lambda repo, question, context_types: chat_with_context(
            "ubicloud", repo, question, context_types),
        inputs=[repo, question, context_types],
        outputs=[output_ubicloud_with_context,
                 output_ubicloud_with_context_prompt]
    )

    question.submit(
        fn=lambda repo, question: chat_without_context(
            "openai", repo, question),
        inputs=[repo, question],
        outputs=output_openai_no_context
    )
    question.submit(
        fn=lambda repo, question: chat_without_context(
            "ubicloud", repo, question),
        inputs=[repo, question],
        outputs=output_ubicloud_no_context
    )
    question.submit(
        fn=lambda repo, question, context_types: chat_with_context(
            "openai", repo, question, context_types),
        inputs=[repo, question, context_types],
        outputs=[output_openai_with_context, output_openai_with_context_prompt]
    )
    question.submit(
        fn=lambda repo, question, context_types: chat_with_context(
            "ubicloud", repo, question, context_types),
        inputs=[repo, question, context_types],
        outputs=[output_ubicloud_with_context,
                 output_ubicloud_with_context_prompt]
    )

# Launch the Gradio app.
demo.launch()

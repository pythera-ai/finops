"""LLM Service - Optimized Response Generation
Provides intelligent response generation with advanced prompt management, multilingual support, and comprehensive error handling."""

import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from openai import OpenAI

logger = logging.getLogger(__name__)


@dataclass
class LLMConfiguration:
    """Optimized LLM configuration container."""

    base_url: str = "https://da951e65b063.ngrok-free.app/v1"
    api_key: str = "ollama"
    model: str = "gemma3-12b-4bit"
    temperature: float = 0.6
    max_tokens: int = 5000
    top_p: float = 0.6


class LanguageDetector:
    """Intelligent language detection for multilingual support."""

    VIETNAMESE_CHARS = (
        "àáạảãâầấậẩẫăằắặẳẵèéẹẻẽêềếệểễìíịỉĩòóọỏõôồốộổỗơờớợởỡùúụủũưừứựửữỳýỵỷỹđ"
    )
    VIETNAMESE_WORDS = {
        "là",
        "có",
        "không",
        "thế",
        "nào",
        "ở",
        "đâu",
        "gì",
        "như",
        "về",
        "của",
        "một",
        "này",
        "đó",
        "cho",
        "với",
        "được",
        "sẽ",
        "đã",
        "khi",
        "nếu",
        "để",
        "và",
        "hay",
        "hoặc",
        "nhưng",
        "vì",
        "do",
        "theo",
        "từ",
        "trong",
        "trên",
        "dưới",
        "giữa",
        "sau",
        "trước",
    }

    @classmethod
    def detect_language(cls, text: str) -> str:
        """
        Detect language of input text with high accuracy.

        Args:
            text: Input text to analyze

        Returns:
            Detected language ('vietnamese' or 'english')
        """
        text_lower = text.lower()

        # Check for Vietnamese characters
        if any(char in text_lower for char in cls.VIETNAMESE_CHARS):
            return "vietnamese"

        # Check for Vietnamese words
        text_words = set(text_lower.split())
        if text_words.intersection(cls.VIETNAMESE_WORDS):
            return "vietnamese"

        return "english"


class PromptManager:
    """Advanced prompt management with multilingual support."""

    SYSTEM_PROMPT = """You are a professional Q&A assistant designed to provide accurate, concise answers based on the provided reference materials. Your primary role is to help users find specific information quickly and efficiently.

## CRITICAL LANGUAGE RULE **MANDATORY**: You MUST respond in the SAME language as the user's question. This is your highest priority rule.
Examples: - Vietnamese question → Vietnamese response entirely - English question → English response entirely - Other languages → Respond in the same language
## Core Principles **Answer Format:** - Provide direct, factual answers without unnecessary introductions - Keep responses concise and focused on the specific question - Use bullet points or numbered lists only when information naturally requires it - Avoid redundant information or lengthy explanations unless requested
**Source-Based Responses:** - Never speculate or add information beyond what's in the sources - Reference the provided materials when citing information - If conflicting information exists, acknowledge the discrepancy - Distinguish between facts and conditional statements
**Response Guidelines:** - Prioritize factual accuracy over comprehensive coverage - Maintain professional, helpful tone without marketing language - Use clear, straightforward language appropriate for the topic - Only answer based on provided reference materials
## REMEMBER: Always match the user's language in your response."""

    @classmethod
    def build_user_prompt(cls, question: str, context: str, language: str) -> str:
        """
        Build optimized user prompt with enhanced language enforcement.

        Args:
            question: User's question
            context: Reference context
            language: Target language

        Returns:
            Formatted prompt string
        """
        detected_lang = LanguageDetector.detect_language(question)

        prompt_parts = []

        # Language enforcement header
        if detected_lang == "vietnamese":
            prompt_parts.extend(
                [
                    "🚨 NGÔN NGỮ BẮT BUỘC: Bạn PHẢI trả lời hoàn toàn bằng tiếng Việt!",
                    "🚨 MANDATORY LANGUAGE: You MUST respond entirely in Vietnamese!",
                ]
            )
        else:
            prompt_parts.append(
                "🚨 MANDATORY LANGUAGE: You MUST respond entirely in English!"
            )

        prompt_parts.append("\n" + "=" * 60 + "\n")

        # Add reference materials if available
        if context.strip():
            header = (
                "**Tài liệu tham khảo:**"
                if detected_lang == "vietnamese"
                else "**Reference Materials:**"
            )
            prompt_parts.extend([header, context, "\n" + "=" * 50 + "\n"])

        # Add user question
        question_header = (
            "**Câu hỏi của người dùng:**"
            if detected_lang == "vietnamese"
            else "**User Question:**"
        )
        prompt_parts.append(f"{question_header} {question}")

        # Add instructions
        if context.strip():
            if detected_lang == "vietnamese":
                prompt_parts.extend(
                    [
                        "\n**HƯỚNG DẪN QUAN TRỌNG:**",
                        "• Trả lời câu hỏi DỰA HOÀN TOÀN trên tài liệu tham khảo phía trên",
                        "• Nếu không có thông tin trong tài liệu, hãy nói rõ: 'Thông tin được cung cấp không có chi tiết về [chủ đề]'",
                        "• NHỚ: Trả lời hoàn toàn bằng TIẾNG VIỆT, không được dùng tiếng Anh",
                    ]
                )
            else:
                prompt_parts.extend(
                    [
                        "\n**IMPORTANT INSTRUCTIONS:**",
                        "• Answer the question based STRICTLY on the reference materials provided above",
                        "• If information is not available in sources, state: 'The provided information does not contain details about [topic]'",
                        "• REMEMBER: Respond entirely in ENGLISH, do not use Vietnamese",
                    ]
                )
        else:
            fallback_msg = (
                "\n**LƯU Ý:** Không có tài liệu tham khảo. Vui lòng thông báo cho người dùng rằng bạn cần tài liệu tham khảo để đưa ra câu trả lời chính xác. Trả lời bằng TIẾNG VIỆT."
                if detected_lang == "vietnamese"
                else "\n**NOTE:** No reference materials provided. Please inform the user that you need reference materials to provide accurate answers. Respond in ENGLISH."
            )
            prompt_parts.append(fallback_msg)

        return "\n".join(prompt_parts)


class ResponseGenerator:
    """Optimized LLM response generation service."""

    def __init__(self, config: LLMConfiguration):
        self.config = config
        self.client = OpenAI(base_url=config.base_url, api_key=config.api_key)
        self.prompt_manager = PromptManager()

    def generate_response(
        self,
        question: str,
        context: str = "",
        language: str = "Vietnamese",
        history: Optional[List[Dict]] = None,
    ) -> str:
        """
        Generate optimized response using LLM with comprehensive error handling.

        Args:
            question: User's question
            context: Reference context
            language: Target language
            history: Conversation history

        Returns:
            Generated response string
        """
        try:
            # Build message history
            messages = self._build_messages(question, context, language, history)

            # Generate response
            completion = self.client.chat.completions.create(
                model=self.config.model,
                messages=messages,
                temperature=self.config.temperature,
                max_tokens=self.config.max_tokens,
                top_p=self.config.top_p,
                stream=False,
            )

            response = completion.choices[0].message.content
            logger.info(f"Successfully generated response in {language}")

            return response

        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            return self._get_fallback_response(question)

    def _build_messages(
        self, question: str, context: str, language: str, history: Optional[List[Dict]]
    ) -> List[Dict]:
        """Build optimized message history for LLM."""
        messages = []

        # System prompt
        messages.append(
            {"role": "system", "content": self.prompt_manager.SYSTEM_PROMPT}
        )

        # Add conversation history
        if history:
            for entry in history:
                if entry.get("role") in ["user", "assistant"]:
                    messages.append(
                        {"role": entry["role"], "content": entry["content"]}
                    )

        # User prompt with context
        user_prompt = self.prompt_manager.build_user_prompt(question, context, language)
        messages.append({"role": "user", "content": user_prompt})

        return messages

    def _get_fallback_response(self, question: str) -> str:
        """Generate fallback response for error cases."""
        is_vietnamese = LanguageDetector.detect_language(question) == "vietnamese"

        return (
            "Xin lỗi, hệ thống đang gặp sự cố kỹ thuật. Vui lòng thử lại sau."
            if is_vietnamese
            else "Sorry, the system is experiencing technical difficulties. Please try again later."
        )


def main(
    question: str,
    formatted_context: str,
    language: str = "Vietnamese",
    history: Optional[List[Dict]] = None,
    config: Optional[Dict] = None,
) -> str:
    """
    Main LLM service function with comprehensive optimization.

    Args:
        question: User's question
        formatted_context: Formatted context string
        language: Target language
        history: Conversation history
        config: System configuration

    Returns:
        Generated response string
    """
    try:
        # Create optimized LLM configuration
        llm_config = LLMConfiguration(
            model=config.get("llm_model", "gemma3-12b-4bit")
            if config
            else "gemma3-12b-4bit",
            temperature=config.get("temperature", 0.6) if config else 0.6,
            max_tokens=config.get("max_tokens", 5000) if config else 5000,
        )

        # Generate response
        generator = ResponseGenerator(llm_config)
        response = generator.generate_response(
            question=question,
            context=formatted_context,
            language=language,
            history=history,
        )

        return response

    except Exception as e:
        logger.error(f"LLM service failed: {e}")
        raise

from bs4 import BeautifulSoup
import json
from typing import Optional
from openai import OpenAI
from pydantic import BaseModel, Field
import os
import uuid
from dotenv import load_dotenv
import sys

# 환경 변수 로드
load_dotenv()

# 상위 디렉토리 경로 추가
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# BaseCrawler import
from base.base_crawler import BaseCrawler


# Pydantic 모델 정의 - 표준 스키마
class HealthSupportInfo(BaseModel):
    """건강 지원 정보 표준 스키마"""

    id: str = Field(description="고유 ID (UUID)")
    title: str = Field(description="공고/사업/프로그램의 제목(한 줄)")
    support_target: str = Field(
        description="지원 대상 또는 신청/참가 자격을 간결히 요약"
    )
    support_content: str = Field(description="지원 내용/혜택/지원 항목을 핵심만 요약")
    raw_text: Optional[str] = Field(
        default=None, description="원본 텍스트 - 구조화 전 크롤링한 원본 데이터"
    )
    source_url: Optional[str] = Field(default=None, description="출처 URL")
    region: Optional[str] = Field(default=None, description="지역명 (예: 광진구, 전국)")
    target_richness: Optional[int] = Field(
        default=None, description="지원 대상 정보 풍부도 (0~5점)"
    )
    content_richness: Optional[int] = Field(
        default=None, description="지원 내용 정보 풍부도 (0~5점)"
    )


# LLM 응답용 내부 모델 (2가지 케이스로 분리)
# 1. (기존) 단독 실행 시 LLM이 제목까지 찾아야 하는 경우
class _LLMResponseWithTitle(BaseModel):
    """LLM 응답용 (제목 포함)"""

    title: str = Field(description="공고/사업/프로그램의 제목(한 줄)")
    support_target: str = Field(
        description="지원 대상 또는 신청/참가 자격을 간결히 요약"
    )
    support_content: str = Field(description="지원 내용/혜택/지원 항목을 핵심만 요약")
    target_richness: int = Field(description="지원 대상 정보 풍부도 (0~5점)")
    content_richness: int = Field(description="지원 내용 정보 풍부도 (0~5점)")


# 2. (신규) 워크플로우에서 제목을 미리 알려주는 경우
class _LLMResponseNoTitle(BaseModel):
    """LLM 응답용 (제목 제외)"""

    support_target: str = Field(
        description="지원 대상 또는 신청/참가 자격을 간결히 요약"
    )
    support_content: str = Field(description="지원 내용/혜택/지원 항목을 핵심만 요약")
    target_richness: int = Field(description="지원 대상 정보 풍부도 (0~5점)")
    content_richness: int = Field(description="지원 내용 정보 풍부도 (0~5점)")


class LLMStructuredCrawler(BaseCrawler):
    """LLM을 사용하여 크롤링 데이터를 구조화하는 크롤러"""

    def __init__(self, api_key: str = None, model: str = "gpt-4o"):
        """
        Args:
            api_key: OpenAI API 키 (없으면 환경변수에서 가져옴)
            model: 사용할 모델 (gpt-4o, gpt-4o-mini 등)
        """
        super().__init__()  # BaseCrawler 초기화

        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenAI API 키가 필요합니다. 환경변수 OPENAI_API_KEY를 설정하거나 api_key 파라미터를 전달하세요."
            )

        self.client = OpenAI(api_key=self.api_key)
        self.model = model

    def parse_html_file(self, file_path: str) -> BeautifulSoup:
        """로컬 HTML 파일 파싱"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                html_content = f.read()
            return BeautifulSoup(html_content, "html.parser")
        except Exception as e:
            print(f"파일 읽기 실패: {e}")
            return None

    def _extract_text_content(
        self, soup: BeautifulSoup, max_chars: int = 200000
    ) -> str:
        """
        HTML에서 주요 텍스트 내용 추출 (내부 헬퍼)
        - 불필요한 요소(nav, footer, sidebar 등) 제거
        - 메인 콘텐츠 영역 우선 추출
        - 테이블 데이터 구조화

        Args:
            soup: BeautifulSoup 객체
            max_chars: 최대 문자 수 (기본값: 200,000자 = 약 50,000 토큰)

        Returns:
            추출된 텍스트 (길이 제한 적용)
        """
        # 복사본 생성 (원본 soup 수정 방지)
        soup_copy = BeautifulSoup(str(soup), "html.parser")

        # 1️⃣ 불필요한 요소 제거
        unwanted_selectors = [
            "nav",
            "header",
            "footer",
            ".sidebar",
            ".menu",
            ".navigation",
            "#nav",
            "#header",
            "#footer",
            ".ad",
            ".advertisement",
            "script",
            "style",
            "noscript",
            ".cookie-banner",
            ".popup",
        ]

        for selector in unwanted_selectors:
            for element in soup_copy.select(selector):
                element.decompose()

        # 2️⃣ 메인 콘텐츠 영역 찾기
        main_content_selectors = [
            "main",
            "#content",
            "#main",
            ".content",
            ".main-content",
            ".contentArea",
            ".content-area",
            "article",
            ".article",
            "[role='main']",
        ]

        content_area = None
        for selector in main_content_selectors:
            content_area = soup_copy.select_one(selector)
            if content_area:
                break

        # 메인 콘텐츠가 없으면 body 전체 사용
        if not content_area:
            content_area = soup_copy.find("body") or soup_copy

        # 3️⃣ 테이블 데이터 구조화
        text_parts = []

        # 테이블 처리
        for table in content_area.find_all("table"):
            table_lines = ["[표 시작]"]

            # 테이블 헤더
            headers = []
            for th in table.find_all("th"):
                th_text = th.get_text(strip=True)
                if th_text:
                    headers.append(th_text)

            if headers:
                table_lines.append(" | ".join(headers))
                table_lines.append("-" * (len(" | ".join(headers))))

            # 테이블 행
            for row in table.find_all("tr"):
                cells = []
                for cell in row.find_all(["td", "th"]):
                    cell_text = cell.get_text(strip=True)
                    if cell_text:
                        cells.append(cell_text)

                if cells:
                    table_lines.append(" | ".join(cells))

            table_lines.append("[표 끝]\n")

            # 테이블을 문자열로 변환하고 원본에서 제거
            text_parts.append("\n".join(table_lines))
            table.decompose()

        # 4️⃣ 일반 텍스트 추출 (테이블은 이미 제거됨)
        text = content_area.get_text(separator="\n", strip=True)

        # 빈 줄 제거 및 정리
        lines = [line.strip() for line in text.split("\n") if line.strip()]
        general_text = "\n".join(lines)

        # 5️⃣ 테이블 텍스트와 일반 텍스트 결합
        if text_parts:
            cleaned_text = general_text + "\n\n" + "\n\n".join(text_parts)
        else:
            cleaned_text = general_text

        # 6️⃣ 길이 제한 적용
        if len(cleaned_text) > max_chars:
            print(
                f"    ⚠️ 텍스트가 너무 깁니다 ({len(cleaned_text):,}자). {max_chars:,}자로 잘라냅니다."
            )
            cleaned_text = (
                cleaned_text[:max_chars] + "\n\n[... 텍스트가 잘렸습니다 ...]"
            )

        return cleaned_text

    def structure_with_llm(
        self,
        soup: BeautifulSoup,
        title: Optional[str] = None,  # 👈 [수정] title 파라미터 추가
        use_structured_output: bool = True,
    ) -> HealthSupportInfo:
        """
        LLM을 사용하여 BeautifulSoup 객체에서 직접 텍스트를 추출하고 구조화

        Args:
            soup: 크롤링한 BeautifulSoup 객체
            title: (선택) 페이지의 확정된 제목. 제공되면 이 제목을 사용합니다.
            use_structured_output: OpenAI Structured Output 사용 여부
        """

        # 1. soup에서 텍스트 추출
        raw_text = self._extract_text_content(soup)

        # 2. LLM 프롬프트 구성 (title 유무에 따라 분기)
        if title:
            # --- 'title'이 제공된 경우 (워크플로우에서 실행) ---
            system_prompt = f"""당신은 한국어 공고문을 구조적으로 요약하는 보조자 입니다.
당신의 임무는 '{title}'(이)라는 사업에 대한 원문을 읽고, '지원 대상'과 '지원 내용'을 요약하는 것입니다.
'지원 대상'과 '지원 내용'에 대해 요약한 다음 각 필드의 정보 풍부도를 0~5점으로 평가해 주세요. 평가 기준은 '지원 대상'과 '지원 내용'이 얼마나 구체적이고 상세하게 기술되어 있는지에 따라 결정됩니다.
규칙:
- 원문에 근거해 작성하고, 없으면 '정보 없음'으로 기재해 주세요.
- 지원 대상과 지원 내용은 핵심만 요약해 주세요 (길어도 4~6줄 이내).
- 포맷은 제공된 JSON 스키마에 맞춰 반환해 주세요."""

            user_prompt = f"""'{title}' 사업에 대한 원문입니다. '지원 대상'과 '지원 내용'을 추출해 주세요:
================ RAW TEXT ================
{raw_text}
========================================="""

            response_model = _LLMResponseNoTitle  # 👈 제목이 없는 응답 모델

        else:
            # --- 'title'이 제공되지 않은 경우 (단독 실행) ---
            system_prompt = """너는 한국어 공고문을 구조적으로 요약하는 보조자 입니다.
다음 원문에서 '제목', '지원 대상(자격)', '지원 내용'을 꼭 뽑아주세요.
'지원 대상'과 '지원 내용'에 대해 요약한 다음 각 필드의 정보 풍부도를 0~5점으로 평가해 주세요.
규칙:
- 원문에 근거해 작성하고, 없으면 '정보 없음'으로 기재해 주세요.
- 제목(title)은 원문에서 가장 중요한 사업명(H3, H4 등)을 1개만 정확히 추출합니다.
- 지원 대상과 지원 내용은 핵심만 요약해 주세요 (길어도 4~6줄 이내).
- 포맷은 제공된 JSON 스키마에 맞춰 반환해 주세요."""

            user_prompt = f"""다음 원문에서 '제목', '지원 대상', '지원 내용'을 추출해 주세요:
================ RAW TEXT ================
{raw_text}
========================================="""

            response_model = _LLMResponseWithTitle  # 👈 제목이 포함된 응답 모델

        # 3. LLM API 호출
        try:
            if use_structured_output:
                # Structured Output 사용 (더 정확함)
                completion = self.client.beta.chat.completions.parse(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    response_format=response_model,  # 👈 동적 응답 모델 적용
                    temperature=0.1,
                )
                response_data = completion.choices[0].message.parsed

            else:
                # 일반 JSON 모드 사용 (호환성)
                completion = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_prompt},
                    ],
                    response_format={"type": "json_object"},
                    temperature=0.1,
                )
                result_json = json.loads(completion.choices[0].message.content)
                response_data = response_model(**result_json)

            # 4. 최종 HealthSupportInfo 객체 조립
            if title:
                # 'title'이 제공된 경우, 파라미터 'title'을 주입
                return HealthSupportInfo(
                    id=str(uuid.uuid4()),
                    title=title,  # 👈 제공된 title 사용
                    **response_data.model_dump(),
                    raw_text=raw_text,
                )
            else:
                # 'title'이 제공되지 않은 경우, LLM의 응답('title' 포함)을 그대로 사용
                return HealthSupportInfo(
                    id=str(uuid.uuid4()),
                    **response_data.model_dump(),  # 👈 LLM이 찾은 title 사용
                    raw_text=raw_text,
                )

        except Exception as e:
            print(f"LLM 구조화 실패: {e}")
            raise

    def crawl_and_structure(
        self,
        url: str = None,
        file_path: str = None,
        region: str = None,
        title: Optional[str] = None,  # 👈 [수정] title 파라미터 추가
    ) -> HealthSupportInfo:
        """
        웹페이지 또는 파일을 크롤링하고 LLM으로 구조화

        Args:
            url: 크롤링할 URL
            file_path: 로컬 HTML 파일 경로
            region: 지역명 (예: "광진구", "전국")
            title: (선택) 페이지의 확정된 제목.
        """
        # 1. HTML 가져오기
        if url:
            soup = self.fetch_page(url)
            source_url = url
        elif file_path:
            soup = self.parse_html_file(file_path)
            source_url = file_path
        else:
            raise ValueError("url 또는 file_path 중 하나는 필수입니다.")

        if not soup:
            raise ValueError("HTML을 가져올 수 없습니다.")

        # 2. LLM으로 구조화 (soup 객체와 title을 직접 전달)
        # 👈 [수정] title을 structure_with_llm으로 전달
        structured_data = self.structure_with_llm(soup, title=title)

        # 3. 메타 정보 설정
        structured_data.source_url = source_url
        if region:
            structured_data.region = region

        return structured_data

    def save_to_json(self, data: HealthSupportInfo, output_path: str):
        """구조화된 데이터를 JSON으로 저장"""
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data.model_dump(), f, ensure_ascii=False, indent=2)
            print(f"[OK] 데이터가 {output_path}에 저장되었습니다.")
        except Exception as e:
            print(f"[ERROR] 파일 저장 실패: {e}")

    def print_structured_data(self, data: HealthSupportInfo):
        """구조화된 데이터를 보기 좋게 출력"""
        print("\n" + "=" * 80)
        print(f"■ ID: {data.id}")
        print(f"■ 제목: {data.title}")
        if data.region:
            print(f"■ 지역: {data.region}")
        print("=" * 80)

        if data.support_target:
            print("\n■ 지원 대상(자격)")
            self._print_multiline(data.support_target, indent=1)

        if data.support_content:
            print("\n■ 지원 내용")
            self._print_multiline(data.support_content, indent=1)

        if data.source_url:
            print(f"\n■ 출처: {data.source_url}")

        print("\n" + "=" * 80)

    def _print_multiline(self, text: str, indent: int = 0):
        """여러 줄 텍스트를 들여쓰기하여 출력"""
        prefix = "  " * indent
        lines = text.split("\n")
        for line in lines:
            if line.strip():
                print(f"{prefix}{line.strip()}")


def main():
    """메인 실행 함수 (단독 테스트용)"""
    import argparse

    parser = argparse.ArgumentParser(
        description="LLM을 사용하여 의료비 지원 정보를 크롤링하고 구조화합니다."
    )
    parser.add_argument("--url", type=str, help="크롤링할 웹페이지 URL")
    parser.add_argument("--file", type=str, help="크롤링할 로컬 HTML 파일 경로")
    parser.add_argument(
        "--output",
        type=str,
        default="structured_output.json",
        help="출력 JSON 파일 경로",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4o-mini",
        help="사용할 OpenAI 모델 (기본값: gpt-4o-mini)",
    )

    args = parser.parse_args()

    # URL 또는 파일 경로가 없으면 대화형 모드
    if not args.url and not args.file:
        print("\n" + "=" * 80)
        print("LLM 기반 의료비 지원 정보 크롤러")
        print("=" * 80)
        print("\n옵션을 선택하세요:")

        args.url = input("웹페이지 URL을 입력하세요: ").strip()
        args.output = (
            input("출력 파일명 (기본값: structured_output.json): ").strip()
            or "structured_output.json"
        )

    # LLM 크롤러 생성
    crawler = LLMStructuredCrawler(model=args.model)

    print(f"\n{'=' * 80}")
    if args.url:
        print(f"처리 중: {args.url}")
    else:
        print(f"처리 중: {args.file}")
    print(f"{'=' * 80}")

    try:
        # 크롤링 및 구조화
        if args.url:
            # 👈 [수정] main 함수는 title 없이 호출하므로, LLM이 스스로 제목을 찾습니다.
            structured_data = crawler.crawl_and_structure(url=args.url, title=None)
        else:
            structured_data = crawler.crawl_and_structure(
                file_path=args.file, title=None
            )

        # 결과 출력
        crawler.print_structured_data(structured_data)

        # JSON 저장
        crawler.save_to_json(structured_data, args.output)

        print(f"\n[완료] 구조화된 데이터가 {args.output}에 저장되었습니다.")

    except Exception as e:
        print(f"[ERROR] 처리 실패: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()

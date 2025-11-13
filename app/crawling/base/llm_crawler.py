# -*- coding: utf-8 -*-
"""
llm_crawler.py

변경 요약
- ★ 평가를 '요약문만' 기반으로 산출하도록 2단계로 분리
  1) 원문(raw_text) → (title, support_target, support_content) 요약 생성
  2) 요약(support_target, support_content)만 입력 → eval_target(1~10), eval_content(0~10) 산출
- 지역 표현 일반화(OO구민/주민,시민) 유지
- content 평가는 단일 종합점수(0~10) 유지
"""

from bs4 import BeautifulSoup
import json
from typing import Optional
from openai import OpenAI
from pydantic import BaseModel, Field
import os
import uuid
from dotenv import load_dotenv
import sys
import re

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from base.base_crawler import BaseCrawler


# ─────────────────────────────────────────────────────────────────────
# 출력 스키마
# ─────────────────────────────────────────────────────────────────────
class HealthSupportInfo(BaseModel):
    id: str = Field(description="고유 ID (UUID)")
    title: str = Field(description="공고/사업/프로그램의 제목(한 줄)")
    support_target: str = Field(description="지원 대상 또는 신청/참가 자격 요약")
    support_content: str = Field(description="지원 내용/혜택/지원 항목 요약")
    raw_text: Optional[str] = Field(default=None, description="원문 텍스트")
    source_url: Optional[str] = Field(default=None, description="출처 URL")
    region: Optional[str] = Field(default=None, description="지역명 (예: 광진구, 전국)")
    # 평가 점수(요약만 기반)
    eval_target: Optional[int] = Field(default=None, ge=0, le=10, description="지원대상 단일 종합 점수(1~10)")
    eval_content: Optional[int] = Field(default=None, ge=0, le=10, description="지원내용 단일 종합 점수(0~10)")


# ─────────────────────────────────────────────────────────────────────
# 1단계: 요약 생성용 LLM 출력 스키마(점수 없음)
# ─────────────────────────────────────────────────────────────────────
class _LLMSummary(BaseModel):
    title: Optional[str] = None
    support_target: str
    support_content: str


# ─────────────────────────────────────────────────────────────────────
# 2단계: 요약 평가용 LLM 출력 스키마(요약만 기반)
# ─────────────────────────────────────────────────────────────────────
class _LLMEval(BaseModel):
    eval_target: int = Field(ge=0, le=10, description="1~10")
    eval_content: int = Field(ge=0, le=10, description="0~10")


class LLMStructuredCrawler(BaseCrawler):
    """
    2단계 파이프라인:
      (1) raw_text → 요약(title, support_target, support_content)
      (2) 요약만 입력 → eval_target(1~10), eval_content(0~10)
    """

    def __init__(self, api_key: str = None, model: str = "gpt-4o"):
        super().__init__()
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY가 필요합니다.")
        self.client = OpenAI(api_key=self.api_key)
        self.model = model

    # ---------------- HTML 파싱/정리 ----------------
    def parse_html_file(self, file_path: str) -> Optional[BeautifulSoup]:
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                html_content = f.read()
            return BeautifulSoup(html_content, "html.parser")
        except Exception as e:
            print(f"파일 읽기 실패: {e}")
            return None

    def _extract_text_content(self, soup: BeautifulSoup, max_chars: int = 200000) -> str:
        soup_copy = BeautifulSoup(str(soup), "html.parser")
        for selector in [
            "nav","header","footer",".sidebar",".menu",".navigation",
            "#nav","#header","#footer",".ad",".advertisement",
            "script","style","noscript",".cookie-banner",".popup",
        ]:
            for el in soup_copy.select(selector):
                el.decompose()

        content_area = None
        for selector in [
            "main","#content","#main",".content",".main-content",
            ".contentArea",".content-area","article",".article","[role='main']",
        ]:
            content_area = soup_copy.select_one(selector)
            if content_area:
                break
        if not content_area:
            content_area = soup_copy.find("body") or soup_copy

        text_parts = []
        for table in content_area.find_all("table"):
            table_lines = ["[표 시작]"]
            headers = [th.get_text(strip=True) for th in table.find_all("th") if th.get_text(strip=True)]
            if headers:
                header_line = " | ".join(headers)
                table_lines.append(header_line)
                table_lines.append("-" * len(header_line))
            for row in table.find_all("tr"):
                cells = [cell.get_text(strip=True) for cell in row.find_all(["td","th"]) if cell.get_text(strip=True)]
                if cells:
                    table_lines.append(" | ".join(cells))
            table_lines.append("[표 끝]\n")
            text_parts.append("\n".join(table_lines))
            table.decompose()

        text = content_area.get_text(separator="\n", strip=True)
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        general_text = "\n".join(lines)
        cleaned_text = general_text + ("\n\n" + "\n\n".join(text_parts) if text_parts else "")
        if len(cleaned_text) > max_chars:
            print(f"    ⚠️ 텍스트가 너무 깁니다 ({len(cleaned_text):,}자). {max_chars:,}자로 자릅니다.")
            cleaned_text = cleaned_text[:max_chars] + "\n\n[... 텍스트가 잘렸습니다 ...]"
        return cleaned_text

    # ---------------- 지역 일반화 ----------------
    def _generalize_region_terms(self, text: str) -> str:
        if not text:
            return text
        t = text

        # 1) 구 단위: "강북구 구민/주민/거주자" + "강북구민" 모두 → "지역주민"
        t = re.sub(r"([가-힣]+구)\s*(?:주민|구민|거주자)", "지역주민", t)
        t = re.sub(r"([가-힣]+)구민", "지역주민", t)  # 붙여쓴 형태

        # 2) 시 단위: "서울시 시민/주민" + "서울시민" 모두 → "지역주민"
        t = re.sub(r"([가-힣]+시)\s*(?:시민|주민)", "지역주민", t)
        t = re.sub(r"([가-힣]+)시민", "지역주민", t)  # 붙여쓴 형태

        # 3) "○○구 ... 주민" 형태(중간에 구 이름 여러 개 + 구분자) → "지역주민"
        t = re.sub(r"((?:[가-힣]+구[\s·,]+)+)주민", "지역주민", t)

        return t

    # ---------------- 텍스트 정리 ----------------
    def _dedupe_lines(self, text: str) -> str:
        if not text:
            return text
        norm = lambda s: re.sub(r"[ \t]+", " ", re.sub(r"[·•\-\u2022]+", "-", s.strip().lower()))
        seen, out = set(), []
        for ln in text.splitlines():
            if not ln.strip():
                continue
            key = norm(ln)
            if key in seen:
                continue
            seen.add(key)
            out.append(ln.strip())
        return "\n".join(out)

    # ---------------- (1) 요약 생성 ----------------
    def _summarize_from_raw(self, raw_text: str, title_hint: Optional[str]) -> _LLMSummary:
        RULES_SUMMARY = """
너는 한국 복지/보건 사업 문서를 구조적으로 요약한다.

[필드 분리]
- support_target: '누가/어떤 조건으로' (지원 자격)
- support_content: '무엇을/얼마나/어떻게/언제까지' (지원 내용)
- 섞이지 않게 분리. 원문에 없는 조건/수치 생성 금지.
- 대상 신호가 없으면 support_target='정보 없음'.

[출력(JSON만)]
{
  "title": "<제목 또는 null>",
  "support_target": "...",
  "support_content": "..."
}
"""
        sys_prompt = RULES_SUMMARY + (
            "\n제목은 주어졌으므로 새로 추출하지 말 것." if title_hint else
            "\n제목이 명확하지 않으면 본문에서 가장 적절한 사업명을 1개만 추출."
        )
        user_prompt = f"원문:\n{raw_text}"

        # 호출
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": sys_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
            )
            data = json.loads(resp.choices[0].message.content or "{}")
            if title_hint:
                data["title"] = title_hint
            parsed = _LLMSummary(**data)
        except Exception as e:
            print(f"⚠️ 요약 생성 실패: {e}")
            parsed = _LLMSummary(
                title=title_hint or "제목 없음",
                support_target="정보 없음",
                support_content="정보 없음",
            )
        return parsed

    # ---------------- (2) 요약만 평가 ----------------
    def _evaluate_summary_only(self, target_text: str, content_text: str) -> _LLMEval:
        """
        주의: raw_text를 절대 사용하지 않는다.
        오로지 요약된 support_target / support_content만 입력으로 평가한다.
        """
        RULES_EVAL = """
너는 아래 '요약문'만을 근거로 점수를 부여한다. 원문은 보지 않는다. 추론/추가 생성 금지.
- "미취학 아동, 초·중·고등학교 학생, 성인"같은 연속적인 연령군 나열은 단일 조건으로 본다.
- 동시에 충족해야 하는 조건만 복수 정성조건으로 인정한다.
- "지역주민" 단독은 기본점수로 둔다.

[지원대상 eval_target]
1 : 정보 없음
2 : 일반 거주 조건(지역주민/서울시민 등)
3 : 단일 정성 조건 1개(청년/노인/장애/임산부/부부 등)
4 : 일반 거주 조건 + 단일 정성 조건
5 : 단일 정량 조건 1개(소득/병명/기간 등)
6 : 정성 조건 2개
7 : 정량 + 정성 복합조건
8 : 다중 복합조건 or 정성 조건 4개
9 : 명시적 행정기준 포함 복합조건
10 : 예외 조항 포함 복합조건

[지원내용 eval_content — 단일 종합(0~10, 정수)]
- 0 : 지원내용 없음
- 2 : 모호한 서술(‘지원합니다’ 등)
- 4 : 항목은 있으나 금액/기간/횟수 등 핵심 세부 미기재
- 6 : 금액/기간/횟수 중 1개 이상 구체 정보 포함
- 8 : 지원항목 다수 + 절차/제외조건 등 복수 요소 포함
- 10: 금액/횟수/기간/절차/예외 모두 구체적으로 명시


[출력(JSON만)]
{
  "eval_target": 1|2|3|4|5|6|7|8|9|10,
  "eval_content": 0|1|2|3|4|5|6|7|8|9|10
}
"""
        user_prompt = (
            "아래의 두 요약문만을 근거로 평가하라. 원문/외부지식 사용 금지.\n\n"
            f"[지원 대상 요약]\n{target_text or '정보 없음'}\n\n"
            f"[지원 내용 요약]\n{content_text or '정보 없음'}"
        )

        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": RULES_EVAL},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.0,  # 평가 일관성 확보
            )
            data = json.loads(resp.choices[0].message.content or "{}")
            parsed = _LLMEval(**data)
        except Exception as e:
            print(f"⚠️ 요약 평가 실패: {e}")
            parsed = _LLMEval(eval_target=0, eval_content=0)
        return parsed

    # ---------------- 메인 절차 ----------------
    def structure_with_llm(
        self,
        soup: BeautifulSoup,
        title: Optional[str] = None,
        use_structured_output: bool = True,  # 호환용 인자 (미사용)
    ) -> HealthSupportInfo:
        raw_text = self._extract_text_content(soup)

        # 1) 원문 → 요약
        summary = self._summarize_from_raw(raw_text, title_hint=title)

        # 지역 일반화 + 라인 중복 제거
        out_title = summary.title or title or "제목 없음"
        out_target = self._generalize_region_terms(summary.support_target)
        out_content = self._dedupe_lines(self._generalize_region_terms(summary.support_content))

        # 2) 요약만 입력 → 평가
        eval_res = self._evaluate_summary_only(out_target, out_content)

        return HealthSupportInfo(
            id=str(uuid.uuid4()),
            title=out_title,
            support_target=out_target,
            support_content=out_content,
            raw_text=raw_text,                 # 보관은 하되 평가에는 사용하지 않음
            eval_target=int(eval_res.eval_target),
            eval_content=int(eval_res.eval_content),
        )

    # ---------------- 외부 인터페이스 ----------------
    def crawl_and_structure(
        self,
        url: str = None,
        file_path: str = None,
        region: str = None,
        title: Optional[str] = None,
    ) -> HealthSupportInfo:
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

        structured_data = self.structure_with_llm(soup, title=title)
        structured_data.source_url = source_url
        if region:
            structured_data.region = region
        return structured_data

    def save_to_json(self, data: HealthSupportInfo, output_path: str):
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data.model_dump(), f, ensure_ascii=False, indent=2)
            print(f"[OK] 데이터가 {output_path}에 저장되었습니다.")
        except Exception as e:
            print(f"[ERROR] 파일 저장 실패: {e}")

    def print_structured_data(self, data: HealthSupportInfo):
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
        print("\n■ 평가 점수 (요약문만 기준)")
        print(f"  - eval_target (0/2/4/6/8/10): {data.eval_target}")
        print(f"  - eval_content (0~10): {data.eval_content}")
        if data.source_url:
            print(f"\n■ 출처: {data.source_url}")
        print("\n" + "=" * 80)

    def _print_multiline(self, text: str, indent: int = 0):
        prefix = "  " * indent
        for line in text.split("\n"):
            if line.strip():
                print(f"{prefix}{line.strip()}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="LLM으로 정책 텍스트를 요약하고, '요약문만'을 기반으로 점수(타겟 0/2/4/6/8/10, 내용 0~10)를 산출합니다."
    )
    parser.add_argument("--url", type=str, help="크롤링할 웹페이지 URL")
    parser.add_argument("--file", type=str, help="로컬 HTML 파일 경로")
    parser.add_argument(
        "--output",
        type=str,
        default="structured_output.json",
        help="출력 JSON 파일 경로",
    )
    parser.add_argument(
        "--model",
        type=str,
        default="gpt-4o",
        help="사용 모델 (예: gpt-4o, gpt-4o-mini)",
    )
    args = parser.parse_args()

    crawler = LLMStructuredCrawler(model=args.model)
    try:
        if args.url:
            data = crawler.crawl_and_structure(url=args.url)
        else:
            data = crawler.crawl_and_structure(file_path=args.file)
        crawler.print_structured_data(data)
        crawler.save_to_json(data, args.output)
        print(f"\n[완료] {args.output} 저장")
    except Exception as e:
        print(f"[ERROR] 처리 실패: {e}")
        import traceback
        traceback.print_exc()

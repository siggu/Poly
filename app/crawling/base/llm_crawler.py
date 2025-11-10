# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import json
from typing import Optional, Dict
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
# Pydantic 모델: 최종 산출 스키마 (0~10점 스케일 + eval_target/eval_content만 저장용)
# ─────────────────────────────────────────────────────────────────────
class HealthSupportInfo(BaseModel):
    id: str = Field(description="고유 ID (UUID)")
    title: str = Field(description="공고/사업/프로그램의 제목(한 줄)")
    support_target: str = Field(description="지원 대상 또는 신청/참가 자격 요약")
    support_content: str = Field(description="지원 내용/혜택/지원 항목 요약")
    raw_text: Optional[str] = Field(default=None, description="원문 텍스트")
    source_url: Optional[str] = Field(default=None, description="출처 URL")
    region: Optional[str] = Field(default=None, description="지역명 (예: 광진구, 전국)")

    # 0~10 점수(원시 점수는 scores에 유지), 최종 저장용
    eval_target: Optional[int] = Field(
        default=None,
        ge=0,
        le=10,
        description="(2*richness_target + 3*criterion_fit_target)/5 반올림",
    )
    eval_content: Optional[int] = Field(
        default=None,
        ge=0,
        le=10,
        description="(2*richness_content + 3*criterion_fit_content)/5 반올림",
    )

    # 디버깅/분석용(선택): 원시 점수(0~10)
    eval_scores: Optional[Dict] = Field(
        default=None, description="richness/criterion_fit 원시 점수(0~10)"
    )


# LLM 응답 스키마(Structured Output, 0~10)
class _EvalScores(BaseModel):
    richness_target: int = Field(ge=0, le=10)
    richness_content: int = Field(ge=0, le=10)
    criterion_fit_target: int = Field(ge=0, le=10)
    criterion_fit_content: int = Field(ge=0, le=10)


class _LLMResponseWithTitle(BaseModel):
    title: str
    support_target: str
    support_content: str
    scores: _EvalScores


class _LLMResponseNoTitle(BaseModel):
    support_target: str
    support_content: str
    scores: _EvalScores


class LLMStructuredCrawler(BaseCrawler):
    """LLM을 사용하여 크롤링 데이터를 구조화하는 크롤러 (0~10 평가 + 2:3 가중 합성)"""

    def __init__(self, api_key: str = None, model: str = "gpt-4o"):
        super().__init__()
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY가 필요합니다.")
        self.client = OpenAI(api_key=self.api_key)
        self.model = model

    # ---------------- HTML 파싱/정리 ----------------
    def parse_html_file(self, file_path: str) -> BeautifulSoup:
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
        soup_copy = BeautifulSoup(str(soup), "html.parser")
        for selector in [
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
        ]:
            for el in soup_copy.select(selector):
                el.decompose()
        content_area = None
        for selector in [
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
        ]:
            content_area = soup_copy.select_one(selector)
            if content_area:
                break
        if not content_area:
            content_area = soup_copy.find("body") or soup_copy

        text_parts = []
        for table in content_area.find_all("table"):
            table_lines = ["[표 시작]"]
            headers = [
                th.get_text(strip=True)
                for th in table.find_all("th")
                if th.get_text(strip=True)
            ]
            if headers:
                header_line = " | ".join(headers)
                table_lines.append(header_line)
                table_lines.append("-" * len(header_line))
            for row in table.find_all("tr"):
                cells = [
                    cell.get_text(strip=True)
                    for cell in row.find_all(["td", "th"])
                    if cell.get_text(strip=True)
                ]
                if cells:
                    table_lines.append(" | ".join(cells))
            table_lines.append("[표 끝]\n")
            text_parts.append("\n".join(table_lines))
            table.decompose()

        text = content_area.get_text(separator="\n", strip=True)
        lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
        general_text = "\n".join(lines)
        cleaned_text = general_text + (
            "\n\n" + "\n\n".join(text_parts) if text_parts else ""
        )
        if len(cleaned_text) > max_chars:
            print(
                f"    ⚠️ 텍스트가 너무 깁니다 ({len(cleaned_text):,}자). {max_chars:,}자로 자릅니다."
            )
            cleaned_text = (
                cleaned_text[:max_chars] + "\n\n[... 텍스트가 잘렸습니다 ...]"
            )
        return cleaned_text

    # ---------------- 지역 일반화 ----------------
    def _generalize_region_terms(self, text: str) -> str:
        if not text:
            return text
        t = re.sub(r"([가-힣]+구)\s*(주민|구민|거주자)", "지역구민", text)
        t = re.sub(r"([가-힣]+구[\s·,]+)+주민", "지역구민", t)
        return t

    # ---------------- 보조 유틸 ----------------
    _TARGET_KEYS = [
        "지원대상",
        "대상",
        "신청대상",
        "참여대상",
        "이용대상",
        "신청자격",
        "자격요건",
        "자격",
        "해당자",
    ]
    _RESIDENT_KEYS = ["구민", "주민", "거주자", "거주", "주소지", "해당 구"]

    def _contains_numeric_detail(self, text: str) -> bool:
        if not text:
            return False
        return bool(
            re.search(r"(\d[\d,\.]*\s*(원|회|일|개월|주|시간|%|명|가구|건))", text)
        )

    def _dedupe_lines(self, text: str) -> str:
        if not text:
            return text
        norm = lambda s: re.sub(
            r"[ \t]+", " ", re.sub(r"[·•\-\u2022]+", "-", s.strip().lower())
        )
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

    # ---------------- LLM 구조화 ----------------
    def structure_with_llm(
        self,
        soup: BeautifulSoup,
        title: Optional[str] = None,
        use_structured_output: bool = True,
    ) -> HealthSupportInfo:
        raw_text = self._extract_text_content(soup)

        # 0~10 스케일 지시 + 2:3 가중 안내
        common_rules = """
너는 한국 복지/보건 사업 문서를 구조적으로 요약하고 **평가(0~10점)** 하는 보조자다.
반드시 다음을 지켜라:

1) 필드 분리
   - support_target: '누가/어떤 조건으로'
   - support_content: '무엇을/얼마나/어떻게 제공'
   - 섞이면 감점.

2) 지역 일반화/제외
   - 'OO구민/주민' 등은 '지역구민'으로 일반화. 근거 없으면 대상에서 제외.
   - 지역명은 support_target에 넣지 않는다.

3) 추론 금지
   - 원문에 없는 조건/수치/기간을 만들지 말 것.
   - 대상 신호가 없으면 support_target='정보 없음'.

4) 출력(JSON 전용)
{
  "support_target": "...",
  "support_content": "...",
  "scores": {
    "richness_target": 0-10,
    "richness_content": 0-10,
    "criterion_fit_target": 0-10,
    "criterion_fit_content": 0-10
  }
}

5) 채점 가이드(요약)
    [richness_target: '지원대상' 정보의 풍부도(조건·세부성·포괄성)]
    • 0~2  : 대상 정보 부재/한 줄 홍보 문구 수준, 실질적 조건 없음
    • 2~4 : 최소한의 대상 서술만 있고 구체 조건 거의 없음
    • 4~6 : 핵심 조건 2~3개 제시이나 세부 근거/예외/증빙요건 미흡
    • 6~8 : 주요 조건이 대부분 명시, 일부 예외/제외/증빙 안내 포함
    • 8~10: 조건 체계가 완결적(필수/예외/제외/증빙/신청주체 등)이며 용어 정렬

    [richness_content: '지원내용' 정보의 풍부도(항목·수치·절차·범위)]
    • 0~2  : 내용 정보 부재/홍보 문구 중심, 제공 항목 식별 어려움
    • 2~4 : 제공 항목은 있으나 모호(정성적 표현 위주), 정량·단위·상한·기간·방법 누락
    • 4~6 : 항목 1~2개 비교적 구체화했으나 금액/횟수/기간/상한/차등 일부 누락
    • 6~8 : 주요 항목 정리, 금액·횟수·기간·단위 등 다수 포함, 절차 개략 제시
    • 8~10: 항목·범위·단위·상한·지급방식·절차·문의까지 체계적으로 제시, 차등/예외/중복규정 명확

    [criterion_fit_target: '지원대상' 적합도(분리/레이블 일치성)]
    • 0~2  : 대상 항목 부재, 내용과 완전 혼합, 또는 support_target='정보 없음'
    • 3~5 : 대상 항목은 있으나 일부 모호/혼재(내용과 섞임, 레이블 불명확)
    • 6~8 : 대체로 명확하나 소폭 혼재/표현상 혼동 약간
    • 9~10: 완전히 명확, 레이블/내용 일치, 중복·혼재 없음
    ※ support_target='정보 없음'이면 0~30 범위 내 배점

    [criterion_fit_content: '지원내용' 적합도(무엇/얼마나/방법 중심)]
    • 0~2  : 내용 항목 부재, 대상과 완전 혼합
    • 3~5 : 일부만 내용 중심이거나 대상/절차와 섞임, 단편적 나열
    • 6~8 : 대체로 내용 중심(혜택/범위/절차가 구분되나 약간 혼재)
    • 9~10: 내용이 명확히 정리되고 단위·수치·방법이 또렷하며 대상과 분리

6) 가산 규칙(지원내용)
- 금액/횟수/기간 등 정량 정보가 **원문**에 있으면 criterion_fit_content와 richness_content를 상향(과도 금지).
"""

        if title:
            system_prompt = f"""{common_rules}
과제: '{title}' 사업 원문을 읽고 스키마에 맞춰 요약과 0~10점 채점을 산출하라."""
            user_prompt = f"""원문:
================ RAW TEXT ================
{raw_text}
========================================="""
            response_model = _LLMResponseNoTitle
        else:
            system_prompt = f"""{common_rules}
과제: 제목 1개를 추출한 뒤, 스키마에 맞춰 요약과 0~10점 채점을 산출하라."""
            user_prompt = f"""원문:
================ RAW TEXT ================
{raw_text}
========================================="""
            response_model = _LLMResponseWithTitle

        parsed_ok, raw_content = False, None

        # LLM API 호출 시간 측정
        import time

        llm_start = time.time()

        if use_structured_output:
            completion = self.client.beta.chat.completions.parse(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format=response_model,
                temperature=0.1,
            )
            msg = completion.choices[0].message
            raw_content = getattr(msg, "content", None)
            response_data = getattr(msg, "parsed", None)
            parsed_ok = response_data is not None
        else:
            completion = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
            )
            raw_content = completion.choices[0].message.content
            try:
                result_json = json.loads(raw_content)
                response_data = response_model(**result_json)
                parsed_ok = True
            except Exception:
                parsed_ok = False
                response_data = None

        llm_duration = time.time() - llm_start

        # 속도 통계에 기록
        try:
            sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
            import utils

            utils.get_timing_stats().add_timing("3_LLM_API호출", llm_duration)
        except:
            pass  # 통계 기록 실패해도 계속 진행

        if not parsed_ok and raw_content:
            try:
                result_json = json.loads(raw_content)
                response_data = response_model(**result_json)
                parsed_ok = True
            except Exception:
                pass

        if not parsed_ok or response_data is None:
            print("⚠️ LLM 응답 파싱 실패. 스켈레톤으로 진행.")
            empty = _EvalScores(
                richness_target=0,
                richness_content=0,
                criterion_fit_target=0,
                criterion_fit_content=0,
            )
            if title:
                response_data = _LLMResponseNoTitle(
                    support_target="정보 없음",
                    support_content="정보 없음",
                    scores=empty,
                )
            else:
                response_data = _LLMResponseWithTitle(
                    title="제목 없음",
                    support_target="정보 없음",
                    support_content="정보 없음",
                    scores=empty,
                )

        # 후처리: 지역 일반화 + 중복라인 정리
        out_title = title if title else getattr(response_data, "title", "제목 없음")
        out_target = self._generalize_region_terms(response_data.support_target)
        out_content = self._generalize_region_terms(response_data.support_content)
        out_content = self._dedupe_lines(out_content)

        info = HealthSupportInfo(
            id=str(uuid.uuid4()),
            title=out_title,
            support_target=out_target,
            support_content=out_content,
            raw_text=raw_text,
        )

        # 점수 파싱(0~10) + 하한 보정 후 가중 합성(2:3)
        try:
            raw_scores = getattr(response_data, "scores", None)
            if hasattr(raw_scores, "model_dump"):
                scores_dict = raw_scores.model_dump()
            elif isinstance(raw_scores, dict):
                scores_dict = raw_scores
            else:
                scores_dict = {}

            # 정수/범위 보정
            for k in [
                "richness_target",
                "richness_content",
                "criterion_fit_target",
                "criterion_fit_content",
            ]:
                v = int(scores_dict.get(k, 0) or 0)
                scores_dict[k] = max(0, min(10, v))

            # 원문+지원내용이 정량 정보를 실제 포함하면 약한 하한선 부여(0~10 스케일)
            if self._contains_numeric_detail(
                out_content
            ) and self._contains_numeric_detail(raw_text):
                scores_dict["criterion_fit_content"] = max(
                    scores_dict["criterion_fit_content"], 6
                )
                scores_dict["richness_content"] = max(
                    scores_dict["richness_content"], 5
                )

            info.eval_scores = scores_dict  # 디버깅용(선택 저장)

            rt = scores_dict["richness_target"]
            rc = scores_dict["richness_content"]
            ct = scores_dict["criterion_fit_target"]
            cc = scores_dict["criterion_fit_content"]

            # 가중치 2:3 → (2*richness + 3*criterion_fit)/5
            info.eval_target = int(round((2 * rt + 3 * ct) / 5))
            info.eval_content = int(round((2 * rc + 3 * cc) / 5))

        except Exception as e:
            print(f"⚠️ 점수 파싱/합성 에러: {e}")
            info.eval_scores = {
                "richness_target": 0,
                "richness_content": 0,
                "criterion_fit_target": 0,
                "criterion_fit_content": 0,
            }
            info.eval_target = 0
            info.eval_content = 0

        return info

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
        if data.eval_scores:
            print("\n■ 평가 점수(원시, 0~10)")
            for k, v in data.eval_scores.items():
                print(f"  - {k}: {v}")
        if data.eval_target is not None or data.eval_content is not None:
            print("\n■ 저장용 가중 합성 점수(0~10)")
            print(f"  - eval_target : {data.eval_target}")
            print(f"  - eval_content: {data.eval_content}")
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
        description="LLM을 사용하여 의료/복지 사업 텍스트를 구조화합니다."
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
        default="gpt-4o-mini",
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

# app/crawling/run_pipeline.py
# 목적: district, welfare, ehealth 크롤러 + dbuploader + dbgrouper 통합 실행
import os, sys, json, argparse, traceback
from datetime import datetime

# 프로젝트 루트 경로 보정
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, PROJECT_ROOT)

# ─────────────────────────────────────────────
# 1) 크롤러 임포트
# ─────────────────────────────────────────────
from app.crawling.crawlers.district_crawler import HealthCareWorkflow
from app.crawling.crawlers.welfare_crawler import WelfareCrawler
from app.crawling.crawlers.ehealth_crawler import EHealthCrawler
from app.crawling.crawlers import run_all_crawlers as rac

# ─────────────────────────────────────────────
# 2) DB 관련 임포트
# ─────────────────────────────────────────────
from app.dao.db_policy import dbuploader_policy as dbuploader
from app.dao.db_policy import dbgrouper_policy as dbgrouper
from app.dao.utils_db import eprint

# ─────────────────────────────────────────────
# 3) 유틸
# ─────────────────────────────────────────────
def _ensure_dir(p: str):
    os.makedirs(p, exist_ok=True)
    return p

def run_district(urls, out_dir):
    results = []
    for url in urls:
        wf = HealthCareWorkflow(output_dir=out_dir)
        summary = wf.run(start_url=url)
        if summary and summary.get("output_file"):
            results.append(summary["output_file"])
    return results

def run_welfare(out_dir, no_filter=False, max_items=None):
    crawler = WelfareCrawler(output_dir=out_dir)
    crawler.run_workflow(filter_health=not no_filter, max_items=max_items)
    files = [f for f in os.listdir(out_dir) if f.startswith("welfare_structured_data_")]
    files.sort()
    return [os.path.join(out_dir, files[-1])] if files else []

def run_ehealth(out_dir, categories=None, max_pages=None):
    crawler = EHealthCrawler(output_dir=out_dir)
    crawler.run_workflow(categories=categories, max_pages_per_category=max_pages)
    files = [f for f in os.listdir(out_dir) if f.startswith("ehealth_structured_data_")]
    files.sort()
    return [os.path.join(out_dir, files[-1])] if files else []

def upload_to_db(json_paths, reset="none", emb_model="text-embedding-3-small", commit_every=50):
    for path in json_paths:
        argv_backup = sys.argv[:]
        try:
            sys.argv = [
                "dbuploader_policy.py",
                "--file", path,
                "--reset", reset,
                "--model", emb_model,
                "--commit-every", str(commit_every)
            ]
            dbuploader.main()
        finally:
            sys.argv = argv_backup

def group_policies(threshold=0.85, batch_size=500, reset_all=False, verbose=True):
    res = dbgrouper.assign_policy_ids(
        title_field="title",
        similarity_threshold=threshold,
        batch_size=batch_size,
        dry_run=False,
        reset_all_on_start=reset_all,
        verbose=verbose,
    )
    return res

def _get_runall_urls():
    """
    run_all_crawlers.py 안의 URL 리스트를 유연하게 가져온다.
    TARGET_URLS / DISTRICT_TARGETS / DEFAULT_URLS 등 어느 이름이든 대응.
    """
    for name in ["TARGET_URLS", "DISTRICT_TARGETS", "DEFAULT_URLS", "URLS"]:
        if hasattr(rac, name):
            v = getattr(rac, name)
            try:
                return list(v)
            except Exception:
                pass
    return []
# ─────────────────────────────────────────────
# 4) 메인
# ─────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(description="통합 크롤링 → 업로드 → 그루핑 파이프라인")
    p.add_argument("--source", choices=["district", "welfare", "ehealth", "all"], default="district")
    p.add_argument("--urls", nargs="*", help="district 시작 URL들")
    p.add_argument("--out-dir", default=os.path.join(PROJECT_ROOT, "app", "crawling", "output"))
    p.add_argument("--reset", choices=["none", "truncate"], default="none")
    p.add_argument("--group", action="store_true")
    p.add_argument("--threshold", type=float, default=0.85)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--use-runall-targets", action="store_true",
               help="district 수집 시 run_all_crawlers.py에 정의된 URL 목록을 사용")
    args = p.parse_args()

    _ensure_dir(args.out_dir)

    try:
        collected = []
        if args.source in ("district", "all"):
            if args.use_runall_targets:
                urls = _get_runall_urls()
                if not urls:
                    eprint("[district] run_all_crawlers.py에서 URL을 찾지 못했어요. --urls 인자를 사용하세요.")
                    urls = args.urls or []
            else:
                urls = args.urls or [
                    "https://health.gangnam.go.kr/web/business/support/sub01.do",  # 강남구
                    "https://health.gangdong.go.kr/health/site/main/content/GD20030100",  # 강동구
                    "https://www.gangbuk.go.kr/health/main/contents.do?menuNo=400151",  # 강북구
                    "https://www.gangseo.seoul.kr/health/ht020231",  # 강서구
                    "https://www.gwanak.go.kr/site/health/05/10502010600002024101710.jsp",  # 관악구
                    "https://www.gwangjin.go.kr/health/main/contents.do?menuNo=300080",  # 광진구
                    "https://www.guro.go.kr/health/contents.do?key=1320&",  # 구로구
                    "https://www.dongjak.go.kr/healthcare/main/contents.do?menuNo=300342",  # 동작구
                    "https://www.sdm.go.kr/health/contents/infectious/law",  # 서대문구
                    "https://www.seocho.go.kr/site/sh/03/10301000000002015070902.jsp",  # 서초구
                    "https://www.sb.go.kr/bogunso/contents.do?key=6553",  # 성북구
                    "https://www.ydp.go.kr/health/contents.do?key=6073&",  # 영등포구
                    "https://www.songpa.go.kr/ehealth/contents.do?key=4525&",  # 송파구
                    "https://jongno.go.kr/Health.do?menuId=401309&menuNo=401309",  # 종로구
                ]

            eprint(f"[district] {len(urls)}개 URL 처리")
            collected += run_district(urls, args.out_dir)

        if args.source in ("welfare", "all"):
            collected += run_welfare(args.out_dir)

        if args.source in ("ehealth", "all"):
            collected += run_ehealth(args.out_dir)

        if not collected:
            eprint("❌ 수집된 JSON이 없습니다.")
            return

        eprint(f"[upload] {len(collected)}개 JSON 업로드 중…")
        upload_to_db(collected, reset=args.reset)

        if args.group:
            eprint("[group] policy_id 그루핑 시작")
            result = group_policies(args.threshold, args.batch_size)
            print("[group result]", result)

        print("\n✅ 완료:", collected)

    except Exception as e:
        traceback.print_exc()
        eprint(f"오류 발생: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

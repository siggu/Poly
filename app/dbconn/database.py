"""
데이터베이스 연결 및 관리 모듈
PostgreSQL을 사용하여 건강 지원 정보를 저장하고 관리합니다.
"""

import os
from typing import List, Dict, Optional
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Text, Integer, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import UUID
import uuid
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()


class HealthSupportInfo(Base):
    """건강 지원 정보 테이블"""
    __tablename__ = "health_support_info"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    title = Column(String(500), nullable=False)
    source_url = Column(Text, nullable=False)
    region = Column(String(100))
    support_target = Column(Text)
    support_content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # 추가 메타데이터 (JSON 형식으로 저장)
    metadata_json = Column(JSON)
    
    # PDF 관련 필드 (선택적)
    page_number = Column(Integer)
    total_pages = Column(Integer)

    def to_dict(self) -> Dict:
        """모델을 딕셔너리로 변환"""
        return {
            "id": self.id,
            "title": self.title,
            "source_url": self.source_url,
            "region": self.region,
            "support_target": self.support_target,
            "support_content": self.support_content,
            "page_number": self.page_number,
            "total_pages": self.total_pages,
            "metadata_json": self.metadata_json,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
        }


class DatabaseManager:
    """데이터베이스 관리 클래스"""

    def __init__(self, database_url: Optional[str] = None):
        """
        Args:
            database_url: PostgreSQL 연결 URL
                예: postgresql://user:password@localhost:5432/dbname
                None이면 환경변수에서 읽어옴
        """
        if database_url is None:
            # 환경변수에서 DB 연결 정보 읽기
            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "5432")
            db_user = os.getenv("DB_USER", "postgres")
            db_password = os.getenv("DB_PASSWORD", "")
            db_name = os.getenv("DB_NAME", "healthinformer")
            
            database_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        
        self.database_url = database_url
        self.engine = create_engine(database_url, echo=False)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def get_session(self) -> Session:
        """세션 생성"""
        return self.SessionLocal()

    def save_data(self, data: List[Dict], region: Optional[str] = None) -> int:
        """
        데이터를 DB에 저장 (중복 체크 후 저장)

        Args:
            data: 저장할 데이터 리스트
            region: 지역명 (데이터에 없으면 이 값 사용)

        Returns:
            저장된 레코드 수
        """
        session = self.get_session()
        saved_count = 0
        
        try:
            for item in data:
                # ID가 있으면 사용, 없으면 생성
                item_id = item.get("id")
                if not item_id:
                    item_id = str(uuid.uuid4())
                
                # 기존 레코드 확인 (id 또는 source_url로)
                existing = session.query(HealthSupportInfo).filter(
                    HealthSupportInfo.id == item_id
                ).first()
                
                if not existing:
                    # source_url로도 확인
                    existing = session.query(HealthSupportInfo).filter(
                        HealthSupportInfo.source_url == item.get("source_url", "")
                    ).first()

                if existing:
                    # 기존 레코드 업데이트
                    existing.title = item.get("title", existing.title)
                    existing.region = item.get("region") or region or existing.region
                    existing.support_target = item.get("support_target", existing.support_target)
                    existing.support_content = item.get("support_content", existing.support_content)
                    existing.page_number = item.get("page_number")
                    existing.total_pages = item.get("total_pages")
                    existing.updated_at = datetime.utcnow()
                    
                    # 메타데이터 저장
                    metadata = {k: v for k, v in item.items() 
                              if k not in ["id", "title", "source_url", "region", 
                                          "support_target", "support_content", 
                                          "page_number", "total_pages"]}
                    if metadata:
                        existing.metadata_json = metadata
                else:
                    # 새 레코드 생성
                    new_record = HealthSupportInfo(
                        id=item_id,
                        title=item.get("title", ""),
                        source_url=item.get("source_url", ""),
                        region=item.get("region") or region,
                        support_target=item.get("support_target"),
                        support_content=item.get("support_content", ""),
                        page_number=item.get("page_number"),
                        total_pages=item.get("total_pages"),
                    )
                    
                    # 메타데이터 저장
                    metadata = {k: v for k, v in item.items() 
                              if k not in ["id", "title", "source_url", "region", 
                                          "support_target", "support_content", 
                                          "page_number", "total_pages"]}
                    if metadata:
                        new_record.metadata_json = metadata
                    
                    session.add(new_record)
                
                saved_count += 1

            session.commit()
            return saved_count
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def load_data(self, region: Optional[str] = None, limit: Optional[int] = None) -> List[Dict]:
        """
        DB에서 데이터 로드

        Args:
            region: 지역명 필터 (None이면 전체)
            limit: 최대 개수 (None이면 전체)

        Returns:
            데이터 리스트
        """
        session = self.get_session()
        
        try:
            query = session.query(HealthSupportInfo)
            
            if region:
                query = query.filter(HealthSupportInfo.region == region)
            
            if limit:
                query = query.limit(limit)
            
            records = query.all()
            return [record.to_dict() for record in records]
            
        finally:
            session.close()

    def get_by_id(self, item_id: str) -> Optional[Dict]:
        """ID로 단일 레코드 조회"""
        session = self.get_session()
        
        try:
            record = session.query(HealthSupportInfo).filter(
                HealthSupportInfo.id == item_id
            ).first()
            
            return record.to_dict() if record else None
            
        finally:
            session.close()

    def delete_by_region(self, region: str) -> int:
        """
        특정 지역의 모든 데이터 삭제

        Returns:
            삭제된 레코드 수
        """
        session = self.get_session()
        
        try:
            deleted_count = session.query(HealthSupportInfo).filter(
                HealthSupportInfo.region == region
            ).delete()
            
            session.commit()
            return deleted_count
            
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    def get_statistics(self) -> Dict:
        """통계 정보 조회"""
        session = self.get_session()
        
        try:
            total_count = session.query(HealthSupportInfo).count()
            
            # 지역별 통계
            regions = session.query(HealthSupportInfo.region).distinct().all()
            region_stats = {}
            for (region,) in regions:
                if region:
                    count = session.query(HealthSupportInfo).filter(
                        HealthSupportInfo.region == region
                    ).count()
                    region_stats[region] = count
            
            return {
                "total_count": total_count,
                "region_stats": region_stats,
            }
            
        finally:
            session.close()



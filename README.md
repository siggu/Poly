<<<<<<< HEAD
requirement.txt 

#torch==2.5.1 -> torch==2.7.1+cu118
#torchaudio==2.7.1+cu118
#torchvision==0.22.1+cu118

pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu118

추가

conda install -c conda-forge faiss-gpu=1.8.0 cudatoolkit=11.8
pip install beatyfulsoup4

=======
requirement 변경사항

streamlit>=1.28.0
 <br />
fastapi>=0.104.0
 <br />
uvicorn[standard]>=0.24.0
 <br />
python-multipart>=0.0.6
 <br />
python-jose[cryptography]>=3.3.0
 <br />
passlib[bcrypt]>=1.7.4
<br/>
pydantic[email]==2.11.7
<br/>
bcrypt==4.0.1



# 모듈 추가 업그레이드 11.14 (fastapi오류로 인한)

[notice] A new release of pip is available: 25.2 -> 25.3
<br/>
[notice] To update, run: python.exe -m pip install --upgrade pip
<br/>
python.exe -m pip install --upgrade pip
<br/>


pip install passlib
<br/>
pip install fastapi
<br/>
pip install jose
<br/>
pip install --upgrade python-jose
<br/>
pip install --upgrade -r requirements.txt
>>>>>>> 40bc580c4edf395c4680a118efa78eaab2ca0746

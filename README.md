# MusicChatBot
입력받은 가수 정보를 바탕으로 관련 음악을 추천해주는 챗봇 서비스입니다.

![KakaoTalk_20230208_165452044](https://user-images.githubusercontent.com/79965168/217502818-12e9201e-54c3-4632-9961-a635b9199daf.jpg)

[채팅 바로가기](https://pf.kakao.com/_vzbrxj/chat)

# 💡 Topic

- Spotify에서 제공하는 Open API와 카카오 챗봇 서비스를 이용하여 사용자가 입력한 노래의 가수 데이터를 바탕으로 연관 가수를 추천하는 프로젝트
- Reference: [음악추천 챗봇0. 서비스기획과 아키텍쳐 설계(Serverless)](https://pearlluck.tistory.com/473)

# ⭐️ Key Function

- 가수 소개
    - 입력한 가수에 대한 Spotify 앱내에서의 Follwer, Popularity 정보와 함께 이미지를 보여줍니다.
- 가수 대표곡 소개
    - 입력한 가수에 대한 Spotify 앱 내에서의 Top 3 트랙을 검색하여 보여줍니다.
- 연관 가수 소개
    - 입력한 가수와 타 가수들의 Track 상세 정보 (audio Feature)를 바탕으로 거리를 계산하여, 거리가 가까운 (연관된) 가수와 해당 가수의 Track 정보를 보여줍니다.
    - 
# ✈️ Airflow Dags
<img src="https://user-images.githubusercontent.com/79965168/217505299-7a9100bc-2d88-4185-bbf7-c059c2af25bf.png"  width="1200" height="400"/>


# 📷 Screenshot

<p>
<img src="https://user-images.githubusercontent.com/79965168/217503095-9818f486-f9b8-4009-a97f-a5b89b6ef635.png"  width="400" height="800"/>
<img src="https://user-images.githubusercontent.com/79965168/217503083-3eb6e12c-768c-478c-bb96-db5274c23fb6.png"  width="400" height="800"/>

</p>

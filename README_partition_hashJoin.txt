1) 처리 흐름(큰 단계)
Phase 0: 초기 계산/예산 수립

B_L, B_R로 블록당 최대 레코드 수(max_left_recs, max_right_recs)를 계산한다.

max_mem이 있으면, 그 안에서 다음을 동시에 만족하도록 예산을 나눈다.

고정 블록 버퍼/포인터 배열

stdio I/O 버퍼(setvbuf에 줄 버퍼)

파티션 파일용 버퍼(2P개: left P개 + right P개)

해시 테이블(아레나) 메모리

그리고 **P(파티션 개수)**를 결정한다.

직관적으로: max_mem이 작을수록 파티션당 목표 크기를 작게 잡아야 하므로 P가 커진다.

P가 커질수록 각 파티션의 Right 데이터가 작아져서 메모리에 올려 해시 테이블 만들기 쉬워진다(대신 임시 파일 개수/쓰기량 증가).

Phase 1: 원본 → 파티션 파일로 분해(Partitioning)

Left와 Right를 각각 원본 파일을 블록 단위로 순차 읽으면서 파티션 파일에 쓴다.

Left 분해

Left 원본 파일을 열고,

fill_block()으로 B_L 크기 블록을 채우고(= 한 블록에 여러 레코드),

각 레코드에서 조인 키를 뽑아 part = hash(key) % P 계산,

해당 left_part_part.tmp에 레코드를 기록한다.

Right 분해

Right 원본도 동일하게,

fill_block()으로 B_R 단위로 읽고,

같은 hash(key)%P 규칙으로 대응되는 right_part_part.tmp에 기록한다.

동시에 right_part_rec_counts[part] 같은 식으로 파티션별 Right 레코드 수를 센다(Phase2에서 메모리/버킷 수 계산용).

Phase1이 끝나면:

파티션 파일들은 “조인 키 기준으로 같은 파티션끼리만 매칭 가능한 상태”가 된다.

원본 파일은 각각 딱 1번 순차 스캔한 것이다.

Phase 2: 파티션별 조인(Partition-wise Hash Join)

p=0..P-1에 대해 반복한다. 핵심은 아래 순서다.

(A) Right 파티션이 비어 있으면(=Right_recs=0)

right_part_rec_counts[p]==0이면,

Left 파티션을 읽으면서(블록 단위) 전부 NULL 결과로 출력한다.

해시테이블 자체를 만들 필요가 없다.

(B) Right 파티션이 있으면

아레나 reset

해시테이블은 파티션마다 새로 만든다.

이전 파티션의 테이블은 버리고, 같은 아레나 메모리를 재사용한다.

Right 파티션을 읽어 해시테이블 구축(Build)

right_part_p.tmp를 블록 단위로 읽는다.

각 레코드에서 key를 뽑고, 버킷에 (key, nonkey)를 넣는다.

충돌 대비로 보통:

해시값(또는 key 문자열)로 먼저 필터링하고

최종적으로 문자열 비교로 같은 키인지 확정한다.

Left 파티션을 읽어 Probe + 출력

left_part_p.tmp를 블록 단위로 읽는다.

각 left 레코드에 대해 key로 해시테이블을 probe한다.

매칭되면 left + right_nonkey를 출력한다.

매칭이 하나도 없으면 left + NULL ...을 출력한다.
(Left join이므로 “left는 반드시 1번은 출력”)

Phase2가 끝나면 모든 파티션 처리가 완료되고, join 결과 전체가 완성된다.

2) 메모리 사용 방식(무엇을 잡고, 어디에 쓰는가)

이 방식은 chunk 방식과 “메모리를 극도로 제한”하는 포인트가 조금 다르다. 핵심은 동시에 메모리에 올리는 Right의 범위를 “전체”가 아니라 “파티션 하나”로 강제한다는 것.

A. 고정(big_alloc)로 잡아두는 것(계속 재사용)

블록 버퍼: blkL, blkR (left/right 읽을 때 재사용)

레코드 포인터 배열: recsL, recsR

stdio 버퍼:

output 버퍼

원본 입력 버퍼(Left/Right)

파티션 파일 버퍼(2P개를 위한 pool)

이들은 실행 내내 유지되고, 로그의 fixed_big_alloc에 누적된다(네 시스템에서 big_alloc이 추적한다면).

B. 해시테이블 아레나(arena) — 파티션마다 reset

arena_cap만큼을 한 번 확보하고,

파티션 처리 때마다 reset해서 재사용한다.

Right 파티션을 테이블로 만들 때 필요한 메모리는 항상 이 아레나 안에서만 쓰게 된다.

그래서 max_mem이 작으면 P를 늘려 Right 파티션 크기를 줄여 아레나 안에 들어오도록 만든다.

C. 파티션 메타데이터(포인터 배열, FILE* 배열, 경로 문자열 등)

코드에 따라 malloc/free로 잡는 부분이 있을 수 있다.

“엄격한 max_mem 통제”를 목표로 한다면, 이 메타데이터도 big_alloc로 바꾸거나, 적어도 로그에서 “제한에 포함/미포함”을 구분해 표시하는 게 좋다.

(너의 최신 로그가 strict alloc이라고 찍히는 버전은 보통 이런 부분까지 예산에 반영하도록 조정한 형태임)

3) 블록 사용 방식(Left/Right가 실제로 어떻게 읽히는가)
Phase1(원본 분해)

Left 원본: B_L 블록 단위로 fill_block() → 각 레코드를 어느 파티션 파일로 쓸지 결정

Right 원본: B_R 블록 단위로 fill_block() → 동일 규칙으로 파티션 파일로 기록

즉 원본 파일은 각각 1번만 읽고, 대신 파티션 파일로 대량 쓰기가 발생한다.

Phase2(파티션 조인)

파티션 p마다:

Right 파티션 파일을 블록 단위로 읽으며 해시 구축

Left 파티션 파일을 블록 단위로 읽으며 probe & 출력

즉 Right는 chunk 방식처럼 “pass마다 원본 전체 재스캔”이 아니라,

Right 파티션을 “딱 1번(build)” 읽고

Left 파티션을 “딱 1번(probe)” 읽는 구조가 된다.

4) chunk 방식과 비교되는 핵심 차이(성능/로그 해석 관점)
Chunk + Right full scan 방식

pass가 늘면 Right 전체를 매번 다시 읽는다.

right_scan_passes가 커지고, total_right_blocks가 폭증한다.

Partitioned hash join 방식

Right 전체를 반복 스캔하지 않는다.

대신:

Phase1에서 원본 전체를 파티션 파일로 한 번 분해(쓰기 비용 큼)

Phase2에서 파티션별로 Right를 해시로 만들어 한 번씩만 사용

그래서 로그는 보통:

Phase1 done에서 원본 블록 카운트가 나오고

part=... 별로 Rrecs, buckets 같은 “파티션별 작업량”이 나온다.

5) 네가 찍고 있는 로그가 의미하는 것(Partitioned 버전 기준)

P=...: 파티션 개수. 작을수록 파티션 하나당 데이터가 커짐(메모리 압박↑).

arena_cap=...: 파티션 해시테이블/문자열 저장에 쓰는 메모리 상한.

Phase1 done: left_blocks=... right_blocks=...: 원본을 몇 블록 읽어 파티셔닝했는지.

Partition dist: ... (maxR=...): 파티션 분포. maxR가 매우 크면 특정 파티션에 쏠림(해시/분포 문제 가능).

part=i/P: Lrecs=... Rrecs=... buckets=...: 파티션 i에서 해시 크기와 작업량.

Phase2 done: 파티션 조인에서 실제로 파티션 파일들을 블록 단위로 읽은 총량.

SUMMARY: Phase1+Phase2 합친 총 블록 I/O량(네 구현에선 phase별 합계를 보여줌).
작동 순서(읽기용 정리)
1) 시작 단계: 크기 계산과 “한도 내 메모리 설계”

입력으로 받은 max_mem, B_L, B_R를 기준으로 한 블록에 들어갈 수 있는 최대 레코드 수(max_left_recs, max_right_recs)를 계산한다.

파일의 크기(stat)를 읽어 “대략 몇 블록일지”(est_left_blocks, est_right_blocks)도 계산한다.

이 값은 로그/예상 pass 수 계산용이며, 실제 처리는 런타임 fill_block() 결과가 기준이다.

2) 메모리 할당 방식(무엇을 얼마나 잡는가)

이 구현은 조인 중에 malloc/calloc/strdup 같은 동적 할당을 계속 하지 않게 만들었다.
대신 시작 시점에 **큰 덩어리(big_alloc)**로 필요한 버퍼를 확보하고, 조인 중에는 그 메모리를 재사용한다.

메모리 구성은 크게 3개로 나뉜다.

A. 고정 버퍼(프로그램 끝까지 유지, 재사용)

Right를 읽기 위한 블록 버퍼 1개

Right는 “pass마다 전체를 다시 읽기” 때문에, Right를 담는 블록 버퍼는 1개만 있으면 된다.

Right 블록 안의 레코드 포인터 배열

블록 안에 레코드들이 들어가면, 각 레코드의 시작 주소를 가리키는 포인터를 저장하는 배열이 필요하다.

stdio I/O 버퍼(입력/출력 버퍼)

fopen()으로 연 파일들은 기본적으로 libc가 내부 버퍼를 잡는다.

여기서는 그 버퍼를 우리가 big_alloc으로 제공한다.

그래서 “stdio 버퍼도 메모리 제한(max_mem)에 포함된 상태”가 된다.

B. Left chunk 저장 공간(프로그램 끝까지 잡아두고 pass마다 내용만 교체)

Left는 한 번에 전부 못 올리므로 “chunk 단위”로 여러 블록을 메모리에 올린다.

따라서 chunk에 들어갈 Left 블록 버퍼 N개와,

그 N개 블록들에 대해 레코드 포인터를 담는 큰 포인터 배열(블록 수 × 블록당 최대 레코드 수),

그리고 각 블록마다 “이번 블록에 레코드가 몇 개 들어갔는지”를 기록하는 카운트 배열이 필요하다.

chunk의 최대 블록 수(N)는 max_mem과 “해시 예산(hash_budget)”을 고려해 계산한다.

메모리가 작을수록 N이 작아지고, 따라서 pass 수가 늘어난다.

C. JoinArena(해시 전용 임시 영역, pass마다 reset)

조인에서 해시테이블을 만들 때 노드/버킷/매칭표시 등을 위해 메모리가 필요하다.

이를 위해 “해시 전용 메모리 덩어리”를 big_alloc으로 한 번 확보한다(hash_budget만큼).

매 pass마다 reset해서 같은 메모리를 다시 0부터 쓰는 구조다.

즉, pass가 반복돼도 해시 관련 메모리 사용량은 “고정 상한”을 가진다.

3) 블록 사용 방식(Left와 Right가 어떻게 읽히는가)
Left(왼쪽)

Left 파일은 한 번 열고(pass 루프 전체에서) 계속 읽는다.

매 pass마다:

Left 블록 버퍼들을 순서대로 채운다(가능한 많이, 최대 chunk 용량까지).

더 이상 읽을 게 없으면 마지막 pass에서 종료한다.

결과적으로 Left는 전체 파일을 1번만 순차적으로 읽는다.

다만 “pass” 개념으로 끊어서 처리할 뿐이다.

Right(오른쪽)

Right는 pass마다 파일을 다시 열고 처음부터 끝까지 스캔한다.

Right는 chunk와의 조인을 위해 항상 “현재 pass의 left chunk”에 대해 전체가 필요하므로,

chunk가 50번이면 Right 전체 스캔도 50번이다.

Right는 “블록 버퍼 1개”를 계속 재사용하면서,

블록 하나 채움 → 그 안 레코드들을 probe → 다음 블록 채움… 순서로 진행한다.

4) pass(한 번의 chunk 처리)에서 일어나는 일

pass 하나의 내부 순서는 다음과 같다.

(1) Left chunk 로딩

Left에서 블록들을 최대 chunk_max_blocks개까지 읽어 메모리에 올린다.

각 블록은 “데이터 블록 버퍼”에 저장되고,

블록 안 레코드 주소들은 “포인터 배열”에 저장된다.

이 시점에 “이번 pass에서 처리할 left 레코드 총 개수”가 결정된다.

(2) JoinArena reset

해시 관련 임시 메모리는 pass마다 초기화(reset)된다.

그러면 이전 pass의 해시테이블/매칭표시는 모두 폐기되고,

같은 메모리 공간을 재사용할 수 있다.

(3) matched 배열 생성

left 레코드마다 “매칭 되었는지(0/1)”를 기록할 1바이트 배열을 만든다.

이 배열도 JoinArena에 만든다.

조인 결과가 여러 개 매칭되어도 “matched”는 1로만 표시한다.

(4) left chunk로 해시테이블 구축

left chunk 안의 모든 레코드에 대해:

join key를 뽑아 해시값을 만든다.

해시 버킷에 노드를 추가한다.

이 노드는 “문자열 복사”를 하지 않는다.

left 레코드 문자열은 이미 left 블록 버퍼 안에 들어있고, 그 주소를 가리키는 방식이다.

따라서 해시테이블은 “left 레코드 포인터 + matched 포인터”를 저장하는 구조다.

(5) Right 전체 스캔 + probe

Right 파일을 열고, 블록 1개씩 읽는다.

블록 안 레코드마다:

key 추출 → 해시 → 해당 버킷의 체인을 따라가며 비교한다.

해시값이 같으면 충돌 가능성이 있으므로, 실제 문자열 비교로 key 동일성을 확인한다.

매칭되면 출력 파일에 left + right nonkey를 기록하고, matched를 1로 설정한다.

(6) left chunk에서 unmatched 출력

pass가 끝나면, left 레코드 중 matched가 0인 것들만 골라

right nonkey 자리에 NULL들을 채워서 출력한다.

5) 왜 이 구조에서 메모리 제한이 “엄격”해지는가

조인에 필요한 메모리를 전부 big_alloc로만 확보한다.

big_alloc은 내부 누적량(g_big_alloc_bytes)이 증가하고,

코드가 max_mem을 넘지 않도록 chunk 크기와 hash_budget을 계산한다.

결과적으로:

max_mem을 줄이면 → chunk_max_blocks가 줄어들고 → pass 수가 늘고 → Right 재스캔 횟수가 늘어 시간이 증가한다.

max_mem을 키우면 → chunk_max_blocks가 커져 pass 수가 줄고 → Right 재스캔 횟수가 줄어 시간이 감소한다.

6) 로그가 의미하는 것(핵심 지표)

chunk_max_blocks: 한 pass에 메모리에 올릴 수 있는 left 블록 최대 개수

expected_passes: (추정 left 블록 수) / chunk_max_blocks의 올림값

right_blocks_this_pass: 이번 pass에서 Right를 몇 블록 읽었는지

pass마다 거의 비슷하게 나오면 Right 전체를 매번 읽고 있다는 뜻

SUMMARY: chunk_passes == right_scan_passes:

pass마다 Right 전체 스캔을 수행했다는 구조가 정확히 반영된 것
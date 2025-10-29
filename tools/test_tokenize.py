from indexer.tokenize import tokenize
from indexer import tokenize as tk_module

sample = '''contract MyContract {
using BytesLib for bytes ;
function myFunc ( ) {
bytes memory memBytes = hex "f00dfeed383Fa3B60f9B4AB7fBf6835d3c26C3765cD2B2e2f00dfeed" ;
address addrFromBytes = memBytes . toAddress ( 4 ) ;
// addrFromBytes == 0x383Fa3B60f9B4AB7fBf6835d3c26C3765cD2B2e2
contract MyContract {
using BytesLib for bytes ;
function myFunc ( ) {
bytes memory memBytes = hex "f00d0000000000000000000000000000000000000000000000000000000000000042feed" ;
uint256 uintFromBytes = memBytes . toUint ( 2 ) ;
// uintFromBytes == 42
contract MyContract {
using BytesLib for bytes ;
function myFunc ( ) {
bytes memory memBytes = hex "f00dfeed" ;
bytes memory checkBytesTrue = hex "f00dfeed" ;
bytes memory checkBytesFalse = hex "00000000" ;
bool check1 = memBytes . equal ( checkBytesTrue ) ;
bool check2 = memBytes . equal ( checkBytesFalse ) ;
'''

tokens = tokenize(sample)
print('Total tokens:', len(tokens))
# print first 200 tokens
print(tokens[:200])

# print tokens containing 'f00d' or long zero runs
interesting = [t for t in tokens if 'f00d' in t or set(t) <= set('0123456789') and len(t) > 10]
print('\nInteresting tokens matching f00d or long zeros/digits:')
for t in interesting:
    print(t)

print('\nDebug: show regex matches and spans:')
for m in tk_module.TOKEN_PATTERN.finditer(sample):
    s = m.group(0)
    print(repr(s), 'span=', m.span())

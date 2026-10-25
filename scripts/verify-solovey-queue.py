#!/usr/bin/env python3
"""Verify this core's linear patch queue; no Panel source owns these commits."""
import hashlib
import json
import re
import subprocess
import sys

def git(*args):
    return subprocess.check_output(['git', *args], text=True).strip()

with open('docs/solovey/queue.json', encoding='utf-8') as stream:
    config = json.load(stream)
if len(sys.argv) > 1:
    command = sys.argv[1]
    if command == 'packages':
        assert all(re.fullmatch(r'[./a-zA-Z0-9_-]+', p) for p in config['regressionPackages'])
        print(' '.join(config['regressionPackages']))
    elif command == 'tags':
        print(config['buildTags'])
    elif command == 'companion':
        item = config.get('companion')
        print(item['module'] + '@' + item['version'] if item else '')
    else:
        raise SystemExit('Unknown queue command')
    raise SystemExit(0)
assert config['schema'] == 'solovey.core-patch-queue/v1'
assert git('rev-parse', config['base'] + '^{tree}') == config['upstreamTree'], 'upstream tree differs'
subprocess.check_call(['git', 'merge-base', '--is-ancestor', config['base'], 'HEAD'])
remote = git('ls-remote', config['copyRepository'], config['pristineRef']).split()
assert remote == [config['base'], config['pristineRef']], 'pristine copy differs from upstream base'
commits = git('rev-list', '--reverse', config['base'] + '..HEAD').splitlines()
ids, code = [], []
for commit in commits:
    assert len(git('rev-list', '--parents', '-n', '1', commit).split()) == 2, 'merge in patch queue'
    message = git('show', '-s', '--format=%B', commit)
    patch_id = re.search(r'^Solovey-Patch-ID: (.+)$', message, re.M)
    if patch_id:
        assert 'Solovey-Patch: yes' in message
        for phrase in ['Panel/provider adaptation is insufficient:', 'Regression:', 'Selected upstream base:', 'Contribution status:', 'Removal condition:']:
            assert phrase in message, 'patch rationale missing'
        ids.append(patch_id.group(1))
        code.append(commit)
    else:
        files = git('diff-tree', '--no-commit-id', '--name-only', '-r', commit).splitlines()
        assert files and set(files) <= set(config['metadataPaths']), 'undeclared code outside patch commits'
assert ids == config['patchIds'], 'logical patch order/count differs'
assert git('rev-parse', code[-1] + '^{tree}') == config['codeTree'], 'compiled source/test equivalence differs'
changed = git('diff', '--name-only', code[-1], 'HEAD').splitlines()
assert set(changed) <= set(config['metadataPaths']), 'metadata changed compiled source'
head = git('rev-parse', 'HEAD')
tree = git('rev-parse', 'HEAD^{tree}')
identity = hashlib.sha256(('solovey.core-patch-series/v1\n' + config['base'] + '\n' + '\n'.join(commits) + '\n' + tree + '\n').encode()).hexdigest()
print(json.dumps({'status':'PASS','authority':config['sourceAuthority'],'base':config['base'],'codeTree':config['codeTree'],'head':head,'tree':tree,'enginePatchCount':len(code),'commitCount':len(commits),'commits':commits,'seriesIdentity':identity}, separators=(',', ':')))

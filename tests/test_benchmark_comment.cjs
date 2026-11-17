const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

// Exercise the actual inline github-script body without calling GitHub.
const workflow = fs.readFileSync(
  path.join(__dirname, '../.github/workflows/benchmark-comment.yml'), 'utf8');
const script = workflow.split('          script: |\n')[1]
  .split('\n').map(line => line.slice(12)).join('\n');
const {renderReport, publish} = new Function('require', script.replace(
  'await publish({github, context, core});', 'return {renderReport, publish};'))(require);

function report() {
  return {schema_version: 2, iterations: 6, threshold_pct: 20, threshold_seconds: 0.1, results:
    [1, 4].flatMap(procs => ['small', 'large', 'verbose', 'sequence', 'cache-cold', 'cache-warm'].map(scenario => ({
      scenario, procs, base_s: 1, candidate_s: 1.01, delta_pct: 1,
      mad_pct: {base: 2, candidate: 3}, slow_pairs: 0, status: 'pass',
    })))};
}

test('renders only supported fields, not arbitrary artifact text', () => {
  const data = report();
  data.message = '@everyone malicious artifact text';
  const markdown = renderReport(data);
  assert.match(markdown, /cache-cold/);
  assert.match(markdown, /2.0% \/ 3.0%/);
  assert.ok(!markdown.includes('malicious'));
});

test('rejects malformed, injected, incomplete and duplicate rows', () => {
  for (const mutate of [
    r => {r.results[0].scenario = '@someone';},
    r => {r.results[0].candidate_s = '1 | injected';},
    r => {r.results[0].base_s = 0;},
    r => {r.results[0].mad_pct.base = NaN;},
    r => {r.results[0].status = '[click](https://example.com)';},
    r => {r.results.pop();},
    r => {r.results[1] = r.results[0];},
  ]) {
    const data = report(); mutate(data);
    assert.throws(() => renderReport(data));
  }
});

function fixture({stale = false, fork = false, existing = false} = {}) {
  const calls = [];
  const sha = 'a'.repeat(40);
  const pr = {number: 7, state: 'open', base: {repo: {full_name: 'owner/hashio'}},
              head: {sha: stale ? 'b'.repeat(40) : sha}};
  const github = {
    rest: {
      repos: {listPullRequestsAssociatedWithCommit: 'associated'},
      pulls: {get: async () => ({data: pr})},
      actions: {listWorkflowRunArtifacts: 'artifacts'},
      issues: {
        listComments: 'comments',
        createComment: async args => calls.push(['create', args]),
        updateComment: async args => calls.push(['update', args]),
      },
    },
    paginate: async kind => {
      if (kind === 'associated') return [pr];
      if (kind === 'artifacts') return [];
      if (kind === 'comments') return existing ? [
        {id: 42, user: {login: 'github-actions[bot]'}, body: '<!-- hashio-benchmark-comparison --> old'},
      ] : [];
      throw new Error('Unexpected API call');
    },
  };
  const context = {repo: {owner: 'owner', repo: 'hashio'}, serverUrl: 'https://github.com',
    payload: {workflow_run: {event: 'pull_request', path: '.github/workflows/benchmarks.yml',
      id: 123, head_sha: sha, conclusion: 'failure', pull_requests: fork ? [] : [pr]}}};
  return {github, context, core: {info() {}, warning() {}}, calls};
}

test('skips results for an old PR commit', async () => {
  const f = fixture({stale: true});
  await publish(f);
  assert.equal(f.calls.length, 0);
});

test('finds fork PR associations and reports missing artifacts', async () => {
  const f = fixture({fork: true});
  await publish(f);
  assert.equal(f.calls[0][0], 'create');
  assert.equal(f.calls[0][1].issue_number, 7);
  assert.match(f.calls[0][1].body, /No complete benchmark report/);
});

test('updates an existing bot comment instead of adding another', async () => {
  const f = fixture({existing: true});
  await publish(f);
  assert.equal(f.calls[0][0], 'update');
  assert.equal(f.calls[0][1].comment_id, 42);
});

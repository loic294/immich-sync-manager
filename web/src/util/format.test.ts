import { describe, expect, it } from 'vitest';
import { formatBytes, formatLocation, humanize, percent } from '@/util/format';

describe('format', () => {
  it('formats sizes', () => {
    expect(formatBytes(0)).toBe('0 B');
    expect(formatBytes(1536)).toBe('1.5 KB');
    expect(formatBytes(50 * 1024 * 1024)).toBe('50 MB');
    expect(formatBytes(-1)).toBe('—');
  });

  it('formats locations and labels', () => {
    expect(formatLocation({ root_id: 'photos', path: '' })).toBe('photos:/');
    expect(formatLocation({ root_id: 'photos', path: '2024/a.jpg' })).toBe('photos:2024/a.jpg');
    expect(humanize('changed_on_both')).toBe('Changed on both');
  });

  it('clamps percentages', () => {
    expect(percent(1, 0)).toBe(0);
    expect(percent(5, 4)).toBe(100);
    expect(percent(1, 3)).toBe(33);
  });
});

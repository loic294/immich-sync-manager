import { describe, expect, it } from 'vitest';
import {
  formatBytes,
  formatCountdown,
  formatDuration,
  formatLocation,
  humanize,
  percent,
} from '@/util/format';

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

  it('formats countdowns', () => {
    const now = Date.parse('2024-01-01T00:00:00Z');
    expect(formatCountdown('2024-01-01T00:01:05Z', now)).toBe('1:05');
    expect(formatCountdown('2023-12-31T23:59:00Z', now)).toBe('0:00');
    expect(formatCountdown(null, now)).toBe('—');
  });

  it('formats remaining transfer time without overflowing minute fields', () => {
    expect(formatDuration(6)).toBe('6 sec');
    expect(formatDuration(61)).toBe('2 min');
    expect(formatDuration(7199)).toBe('2 hr 0 min');
    expect(formatDuration(90000)).toBe('1 day 1 hr');
  });
});

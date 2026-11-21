#[derive(Clone, Copy, Default)]
pub struct Click<'a> {
    pub at: i64,
    pub id: &'a [u8],
    pub campaign: u32,
}

pub trait ClickRecord<'a>: Copy + Default + Send {
    fn encode(click: Click<'a>, origin: i64) -> Self;
    fn sort_key(self) -> i64;
    fn time_key(at: i64, origin: i64) -> i64;
    fn timestamp(self, origin: i64) -> i64;
    fn id(self) -> &'a [u8];
    fn campaign(self) -> u32;
}

impl<'a> ClickRecord<'a> for Click<'a> {
    fn encode(click: Click<'a>, _: i64) -> Self { click }
    fn sort_key(self) -> i64 { self.at }
    fn time_key(at: i64, _: i64) -> i64 { at }
    fn timestamp(self, _: i64) -> i64 { self.at }
    fn id(self) -> &'a [u8] { self.id }
    fn campaign(self) -> u32 { self.campaign }
}

#[derive(Clone, Copy, Default)]
pub struct PackedClick<'a> {
    data: u64,
    id: &'a [u8],
}

impl PackedClick<'_> {
    pub fn fits(origin: i64, end: i64) -> bool {
        // The upper bound is excluded, so a window of 2^32 milliseconds
        // still has offsets that all fit in u32.
        end.checked_sub(origin)
            .is_some_and(|width| (1..=i64::from(u32::MAX) + 1).contains(&width))
    }
}

impl<'a> ClickRecord<'a> for PackedClick<'a> {
    fn encode(click: Click<'a>, origin: i64) -> Self {
        let offset = Self::time_key(click.at, origin);
        debug_assert!((0..=i64::from(u32::MAX)).contains(&offset));
        Self {
            data: ((offset as u64) << 32) | u64::from(click.campaign),
            id: click.id,
        }
    }

    fn sort_key(self) -> i64 { (self.data >> 32) as i64 }
    fn time_key(at: i64, origin: i64) -> i64 { at - origin }
    fn timestamp(self, origin: i64) -> i64 { origin + self.sort_key() }
    fn id(self) -> &'a [u8] { self.id }
    fn campaign(self) -> u32 { self.data as u32 }
}

#[cfg(test)]
mod tests {
    use super::{Click, ClickRecord, PackedClick};

    #[test]
    fn exact_offsets_campaigns_and_identity_bytes() {
        let week = 7 * 86_400_000;
        let origin = -week;
        let last_offset = i64::from(u32::MAX);
        assert!(PackedClick::fits(origin, origin + last_offset + 1));
        assert!(!PackedClick::fits(origin, origin + last_offset + 2));
        assert!(!PackedClick::fits(i64::MIN, i64::MAX));
        assert!(!PackedClick::fits(origin, origin));

        let id = b"K click\0";
        for offset in [0, 1, week - 1, week, last_offset] {
            let raw = Click { at: origin + offset, id, campaign: u32::MAX };
            let compact = PackedClick::encode(raw, origin);
            assert_eq!(compact.sort_key(), offset);
            assert_eq!(compact.timestamp(origin), raw.at);
            assert_eq!(compact.campaign(), raw.campaign);
            assert_eq!(compact.id(), raw.id);
        }
        // At a UTC epoch session, origin is the inclusive earliest click.
        assert_eq!(PackedClick::time_key(0, origin) - week, 0);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn compact_layout_shrinks_each_click_by_eight_bytes() {
        assert_eq!(std::mem::size_of::<Click<'_>>(), 32);
        assert_eq!(std::mem::size_of::<PackedClick<'_>>(), 24);
    }
}

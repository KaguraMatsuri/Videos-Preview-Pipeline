use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Recipe {
    pub start_seconds: f64,
    pub duration_seconds: f64,
    pub fps: f64,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub scale: Scale,
    pub fit: Fit,
    pub palette: Palette,
    pub dither: Dither,
    pub colors: u16,
    pub loop_count: i32,
    pub video_stream: u16,
}

macro_rules! option_enum {
    ($name:ident, $default:ident, $($variant:ident),+) => {
        #[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { #[default] $default, $($variant),+ }
    };
}
option_enum!(Scale, Lanczos, Bicubic, Bilinear, FastBilinear, Neighbor);
option_enum!(Fit, Contain, Stretch);
option_enum!(Palette, Default, Optimized);
option_enum!(Dither, Sierra2_4a, Bayer, FloydSteinberg, None);

impl Default for Recipe {
    fn default() -> Self {
        Self {
            start_seconds: 0.0,
            duration_seconds: 5.0,
            fps: 5.0,
            width: None,
            height: Some(240),
            scale: Scale::default(),
            fit: Fit::default(),
            palette: Palette::default(),
            dither: Dither::default(),
            colors: 256,
            loop_count: 0,
            video_stream: 0,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecipeOverrides {
    pub start_seconds: Option<f64>,
    pub duration_seconds: Option<f64>,
    pub fps: Option<f64>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub scale: Option<Scale>,
    pub fit: Option<Fit>,
    pub palette: Option<Palette>,
    pub dither: Option<Dither>,
    pub colors: Option<u16>,
    pub loop_count: Option<i32>,
    pub video_stream: Option<u16>,
}

impl Recipe {
    pub fn overridden(mut self, o: &RecipeOverrides) -> Self {
        macro_rules! set { ($($f:ident),+) => { $(if let Some(v) = o.$f { self.$f = v; })+ }; }
        set!(
            start_seconds,
            duration_seconds,
            fps,
            scale,
            fit,
            palette,
            dither,
            colors,
            loop_count,
            video_stream
        );
        // Zero clears a dimension; at least one dimension must remain configured.
        if let Some(v) = o.width {
            self.width = (v != 0).then_some(v);
        }
        if let Some(v) = o.height {
            self.height = (v != 0).then_some(v);
        }
        self
    }
    pub fn validate(&self) -> Result<()> {
        if !self.start_seconds.is_finite()
            || self.start_seconds < 0.0
            || self.start_seconds > 604800.0
        {
            return Err(Error::Invalid(
                "start_seconds must be between 0 and 604800".into(),
            ));
        }
        if !self.duration_seconds.is_finite()
            || !(0.01..=3600.0).contains(&self.duration_seconds)
            || !self.fps.is_finite()
            || !(0.1..=100.0).contains(&self.fps)
        {
            return Err(Error::Invalid(
                "duration_seconds or fps is outside supported bounds".into(),
            ));
        }
        if (self.width.is_none() && self.height.is_none())
            || [self.width, self.height]
                .into_iter()
                .flatten()
                .any(|v| !(2..=8192).contains(&v))
        {
            return Err(Error::Invalid(
                "configure width or height between 2 and 8192".into(),
            ));
        }
        if !(2..=256).contains(&self.colors) || !(-1..=65535).contains(&self.loop_count) {
            return Err(Error::Invalid("invalid colors or loop_count".into()));
        }
        Ok(())
    }
    pub fn filter(&self) -> String {
        let scale = match self.scale {
            Scale::Lanczos => "lanczos",
            Scale::Bicubic => "bicubic",
            Scale::Bilinear => "bilinear",
            Scale::FastBilinear => "fast_bilinear",
            Scale::Neighbor => "neighbor",
        };
        let width = self
            .width
            .map_or_else(|| "-2".to_owned(), |v| v.to_string());
        let height = self
            .height
            .map_or_else(|| "-2".to_owned(), |v| v.to_string());
        let fit = if self.width.is_some() && self.height.is_some() && self.fit == Fit::Contain {
            ":force_original_aspect_ratio=decrease:force_divisible_by=2"
        } else {
            ""
        };
        let base = format!("fps={},scale={width}:{height}:flags={scale}{fit}", self.fps);
        if self.palette == Palette::Default {
            return base;
        }
        let dither = match self.dither {
            Dither::Sierra2_4a => "sierra2_4a",
            Dither::Bayer => "bayer",
            Dither::FloydSteinberg => "floyd_steinberg",
            Dither::None => "none",
        };
        format!(
            "{base},split[a][b];[a]palettegen=max_colors={}:reserve_transparent=0[p];[b][p]paletteuse=dither={dither}",
            self.colors
        )
    }
}

pub fn digest(bytes: impl AsRef<[u8]>) -> String {
    format!("{:x}", Sha256::digest(bytes.as_ref()))
}

pub fn artifact_key(fingerprint: &str, recipe: &Recipe, version: &str) -> Result<String> {
    Ok(format!(
        "{}.gif",
        digest(serde_json::to_vec(&(version, fingerprint, recipe))?)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn recipe_roundtrip_and_cache_identity_properties() {
        for fps in [0.1, 1.0, 5.0, 24.0, 30.0, 100.0] {
            for height in [2, 240, 720, 8192] {
                let r = Recipe {
                    fps,
                    height: Some(height),
                    ..Recipe::default()
                };
                r.validate().unwrap();
                assert_eq!(
                    r,
                    serde_json::from_slice::<Recipe>(&serde_json::to_vec(&r).unwrap()).unwrap()
                );
                let key = artifact_key("a", &r, "v1").unwrap();
                assert_ne!(key, artifact_key("b", &r, "v1").unwrap());
                assert_ne!(key, artifact_key("a", &r, "v2").unwrap());
                assert_eq!(key.len(), 68);
                assert!(!key.contains('/'));
            }
        }
    }
    #[test]
    fn rejects_non_finite_and_invalid_values() {
        for fps in [f64::NAN, f64::INFINITY, -1.0, 0.0, 101.0] {
            assert!(
                Recipe {
                    fps,
                    ..Recipe::default()
                }
                .validate()
                .is_err()
            );
        }
        let a = Recipe::default();
        let b = Recipe {
            duration_seconds: 4.0,
            ..a.clone()
        };
        assert_ne!(
            artifact_key("a", &a, "v1").unwrap(),
            artifact_key("a", &b, "v1").unwrap()
        );
    }
}

"""Tests for FITS header key constants to ensure they never change."""

import pytest

import irsol_data_pipeline.io.fits.constants as constants


@pytest.mark.parametrize(
    "const_obj,expected_value",
    [
        (constants.FITS_KEY_INSTPF, "INSTPF"),
        (constants.FITS_KEY_MODTYPE, "MODTYPE"),
        (constants.FITS_KEY_SEQLEN, "SEQLEN"),
        (constants.FITS_KEY_SBSEQLN, "SBSEQLN"),
        (constants.FITS_KEY_SBSEQNM, "SBSEQNM"),
        (constants.FITS_KEY_STOKVEC, "STOKVEC"),
        (constants.FITS_KEY_IMGLST, "IMGLST"),
        (constants.FITS_KEY_IMGTYPE, "IMGTYPE"),
        (constants.FITS_KEY_IMGTYPX, "IMGTYPX"),
        (constants.FITS_KEY_IMGTYPY, "IMGTYPY"),
        (constants.FITS_KEY_GUIDST, "GUIDST"),
        (constants.FITS_KEY_PIGINT, "PIGINT"),
        (constants.FITS_KEY_SOLAR_XY, "SOLAR_XY"),
        (constants.FITS_KEY_LMGST, "LMGST"),
        (constants.FITS_KEY_PLCST, "PLCST"),
        (constants.FITS_KEY_CAMPOS, "CAMPOS"),
        (constants.FITS_KEY_SPALPH, "SPALPH"),
        (constants.FITS_KEY_SPGRTWL, "SPGRTWL"),
        (constants.FITS_KEY_SPORD, "SPORD"),
        (constants.FITS_KEY_SPSLIT, "SPSLIT"),
        (constants.FITS_KEY_DRCSYS, "DRCSYS"),
        (constants.FITS_KEY_DRANGL, "DRANGL"),
        (constants.FITS_KEY_DROFFS, "DROFFS"),
        (constants.FITS_KEY_TCUMODE, "TCUMODE"),
        (constants.FITS_KEY_TCURTRN, "TCURTRN"),
        (constants.FITS_KEY_TCURTRP, "TCURTRP"),
        (constants.FITS_KEY_TCUPOSN, "TCUPOSN"),
        (constants.FITS_KEY_REDSOFT, "REDSOFT"),
        (constants.FITS_KEY_REDSTAT, "REDSTAT"),
        (constants.FITS_KEY_REDFILE, "REDFILE"),
        (constants.FITS_KEY_REDNFIL, "REDNFIL"),
        (constants.FITS_KEY_REDDCFL, "REDDCFL"),
        (constants.FITS_KEY_REDDCFT, "REDDCFT"),
        (constants.FITS_KEY_REDDMOD, "REDDMOD"),
        (constants.FITS_KEY_REDROWS, "REDROWS"),
        (constants.FITS_KEY_REDMODE, "REDMODE"),
        (constants.FITS_KEY_REDTCUM, "REDTCUM"),
        (constants.FITS_KEY_REDPIXR, "REDPIXR"),
        (constants.FITS_KEY_REDONAM, "REDONAM"),
        (constants.FITS_KEY_ZCSOFT, "ZCSOFT"),
        (constants.FITS_KEY_ZCFILE, "ZCFILE"),
        (constants.FITS_KEY_ZCSTAT, "ZCSTAT"),
        (constants.FITS_KEY_ZCDESC, "ZCDESC"),
        (constants.FITS_KEY_FFSTAT, "FFSTAT"),
        (constants.FITS_KEY_GLBNOISE, "GLBNOISE"),
        (constants.FITS_KEY_GLBMEAN, "GLBMEAN"),
        (constants.FITS_KEY_SLTANGL, "SLTANGL"),
    ],
)
def test_fits_constants_unchanged(const_obj, expected_value):
    assert const_obj == expected_value

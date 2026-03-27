"""FITS header key constants for the irsol_data_pipeline FITS I/O layer.

Every keyword written by :func:`~irsol_data_pipeline.io.fits.exporter.write_stokes_fits`
and read back by :func:`~irsol_data_pipeline.io.fits.importer.load_fits_measurement`
is defined here so that the two modules share an unambiguous vocabulary and
changes to a key name only need to be made in one place.

Standard FITS keywords (e.g. ``TELESCOP``, ``INSTRUME``, ``DATE-BEG``) are not
redefined here; only the pipeline-specific extensions live in this module.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Top-level measurement fields
# ---------------------------------------------------------------------------

FITS_KEY_INSTPF = "INSTPF"  # instrument_post_focus
FITS_KEY_MODTYPE = "MODTYPE"  # modulator_type
FITS_KEY_SEQLEN = "SEQLEN"  # sequence_length
FITS_KEY_SBSEQLN = "SBSEQLN"  # sub_sequence_length
FITS_KEY_SBSEQNM = "SBSEQNM"  # sub_sequence_name
FITS_KEY_STOKVEC = "STOKVEC"  # stokes_vector
FITS_KEY_IMGLST = "IMGLST"  # images (space-separated integers)
FITS_KEY_IMGTYPE = "IMGTYPE"  # image_type
FITS_KEY_IMGTYPX = "IMGTYPX"  # image_type_x
FITS_KEY_IMGTYPY = "IMGTYPY"  # image_type_y
FITS_KEY_GUIDST = "GUIDST"  # guiding_status
FITS_KEY_PIGINT = "PIGINT"  # pig_intensity
FITS_KEY_SOLAR_XY = "SOLAR_XY"  # solar_disc_coordinates  (e.g. "344.5 447.0")
FITS_KEY_LMGST = "LMGST"  # limbguider_status
FITS_KEY_PLCST = "PLCST"  # polcomp_status

# ---------------------------------------------------------------------------
# Camera sub-model
# ---------------------------------------------------------------------------

FITS_KEY_CAMPOS = "CAMPOS"  # camera.position

# ---------------------------------------------------------------------------
# Spectrograph sub-model
# ---------------------------------------------------------------------------

FITS_KEY_SPALPH = "SPALPH"  # spectrograph.alpha
FITS_KEY_SPGRTWL = "SPGRTWL"  # spectrograph.grtwl
FITS_KEY_SPORD = "SPORD"  # spectrograph.order
FITS_KEY_SPSLIT = "SPSLIT"  # spectrograph.slit (raw mm value)

# ---------------------------------------------------------------------------
# Derotator sub-model
# ---------------------------------------------------------------------------

FITS_KEY_DRCSYS = "DRCSYS"  # derotator.coordinate_system
FITS_KEY_DRANGL = "DRANGL"  # derotator.position_angle
FITS_KEY_DROFFS = "DROFFS"  # derotator.offset

# ---------------------------------------------------------------------------
# TCU sub-model
# ---------------------------------------------------------------------------

FITS_KEY_TCUMODE = "TCUMODE"  # tcu.mode
FITS_KEY_TCURTRN = "TCURTRN"  # tcu.retarder_name
FITS_KEY_TCURTRP = "TCURTRP"  # tcu.retarder_wl_parameter
FITS_KEY_TCUPOSN = "TCUPOSN"  # tcu.positions

# ---------------------------------------------------------------------------
# Reduction sub-model
# ---------------------------------------------------------------------------

FITS_KEY_REDSOFT = "REDSOFT"  # reduction.software
FITS_KEY_REDSTAT = "REDSTAT"  # reduction.status  (stored as bool T/F)
FITS_KEY_REDFILE = "REDFILE"  # reduction.file
FITS_KEY_REDNFIL = "REDNFIL"  # reduction.number_of_files
FITS_KEY_REDDCFL = "REDDCFL"  # reduction.file_dc_used
FITS_KEY_REDDCFT = "REDDCFT"  # reduction.dcfit
FITS_KEY_REDDMOD = "REDDMOD"  # reduction.demodulation_matrix
FITS_KEY_REDROWS = "REDROWS"  # reduction.order_of_rows (space-separated ints)
FITS_KEY_REDMODE = "REDMODE"  # reduction.mode
FITS_KEY_REDTCUM = "REDTCUM"  # reduction.tcu_method
FITS_KEY_REDPIXR = "REDPIXR"  # reduction.pixels_replaced
FITS_KEY_REDONAM = "REDONAM"  # reduction.outfname

# ---------------------------------------------------------------------------
# CalibrationInfo sub-model (ZIMPOL calibration — not wavelength CalibrationResult)
# ---------------------------------------------------------------------------

FITS_KEY_ZCSOFT = "ZCSOFT"  # calibration.software
FITS_KEY_ZCFILE = "ZCFILE"  # calibration.file
FITS_KEY_ZCSTAT = "ZCSTAT"  # calibration.status  (stored as bool T/F)
FITS_KEY_ZCDESC = "ZCDESC"  # calibration.description

# ---------------------------------------------------------------------------
# Top-level flags
# ---------------------------------------------------------------------------

FITS_KEY_FFSTAT = "FFSTAT"  # flatfield_status  (stored as bool T/F)
FITS_KEY_GLBNOISE = "GLBNOISE"  # global_noise
FITS_KEY_GLBMEAN = "GLBMEAN"  # global_mean

# ---------------------------------------------------------------------------
# Solar orientation
# ---------------------------------------------------------------------------

FITS_KEY_SLTANGL = "SLTANGL"  # SolarOrientationInfo.slit_angle_solar_deg

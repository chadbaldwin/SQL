GO
IF OBJECT_ID('dbo.uf_ShowBadCharacters') IS NOT NULL DROP FUNCTION dbo.uf_ShowBadCharacters;
GO
-- =============================================
-- Description:	Replaces "bad" ascii characters with readable identifiers
-- Reference : http://www.asciitable.com/ for decoding identifiers
-- =============================================
CREATE FUNCTION dbo.uf_ShowBadCharacters (
	@InputString varchar(200)
)
RETURNS varchar(500)
AS
BEGIN
	DECLARE @Output varchar(500);

	SET @Output =
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(--REPLACE(
		REPLACE(

		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(

		@InputString

		--Standard ASCII
		,CHAR(0)  ,  '[NUL_0]'),CHAR(1) , '[SOH_1]'),CHAR(2) , '[STX_2]'),CHAR(3) , '[ETX_2]'),CHAR(4) , '[EOT_4]'),CHAR(5) , '[ENQ_5]'),CHAR(6) , '[ACK_6]'),CHAR(7) , '[BEL_7]'),CHAR(8) ,  '[BS_8]'),CHAR(9) , '[TAB_9]')
		,CHAR(10) ,  '[LF_10]'),CHAR(11), '[VT_11]'),CHAR(12), '[FF_12]'),CHAR(13), '[CR_12]'),CHAR(14), '[SO_14]'),CHAR(15), '[SI_15]'),CHAR(16),'[DLE_16]'),CHAR(17),'[DC1_17]'),CHAR(18),'[DC2_18]'),CHAR(19),'[DC3_19]')
		,CHAR(20) , '[DC4_20]'),CHAR(21),'[NAK_21]'),CHAR(22),'[SYN_22]'),CHAR(23),'[ETB_22]'),CHAR(24),'[CAN_24]'),CHAR(25), '[EM_25]'),CHAR(26),'[SUB_26]'),CHAR(27),'[ESC_27]'),CHAR(28), '[FS_28]'),CHAR(29), '[GS_29]')
		,CHAR(30) ,  '[RS_30]'),CHAR(31), '[US_31]')--,CHAR(32), '[SP_32]') --Space
		,CHAR(127),'[DEL_127]')
		--Extended ASCII
		,CHAR(128),'[EXT_128]'),CHAR(129),'[EXT_129]'),CHAR(130),'[EXT_130]'),CHAR(131),'[EXT_131]'),CHAR(132),'[EXT_132]'),CHAR(133),'[EXT_133]'),CHAR(134),'[EXT_134]'),CHAR(135),'[EXT_135]'),CHAR(136),'[EXT_136]'),CHAR(137),'[EXT_137]')
		,CHAR(138),'[EXT_138]'),CHAR(139),'[EXT_139]'),CHAR(140),'[EXT_140]'),CHAR(141),'[EXT_141]'),CHAR(142),'[EXT_142]'),CHAR(143),'[EXT_143]'),CHAR(144),'[EXT_144]'),CHAR(145),'[EXT_145]'),CHAR(146),'[EXT_146]'),CHAR(147),'[EXT_147]')
		,CHAR(148),'[EXT_148]'),CHAR(149),'[EXT_149]'),CHAR(150),'[EXT_150]'),CHAR(151),'[EXT_151]'),CHAR(152),'[EXT_152]'),CHAR(153),'[EXT_153]'),CHAR(154),'[EXT_154]'),CHAR(155),'[EXT_155]'),CHAR(156),'[EXT_156]'),CHAR(157),'[EXT_157]')
		,CHAR(158),'[EXT_158]'),CHAR(159),'[EXT_159]'),CHAR(160),'[EXT_160]'),CHAR(161),'[EXT_161]'),CHAR(162),'[EXT_162]'),CHAR(163),'[EXT_163]'),CHAR(164),'[EXT_164]'),CHAR(165),'[EXT_165]'),CHAR(166),'[EXT_166]'),CHAR(167),'[EXT_167]')
		,CHAR(168),'[EXT_168]'),CHAR(169),'[EXT_169]'),CHAR(170),'[EXT_170]'),CHAR(171),'[EXT_171]'),CHAR(172),'[EXT_172]'),CHAR(173),'[EXT_173]'),CHAR(174),'[EXT_174]'),CHAR(175),'[EXT_175]'),CHAR(176),'[EXT_176]'),CHAR(177),'[EXT_177]')
		,CHAR(178),'[EXT_178]'),CHAR(179),'[EXT_179]'),CHAR(180),'[EXT_180]'),CHAR(181),'[EXT_181]'),CHAR(182),'[EXT_182]'),CHAR(183),'[EXT_183]'),CHAR(184),'[EXT_184]'),CHAR(185),'[EXT_185]'),CHAR(186),'[EXT_186]'),CHAR(187),'[EXT_187]')
		,CHAR(188),'[EXT_188]'),CHAR(189),'[EXT_189]'),CHAR(190),'[EXT_190]'),CHAR(191),'[EXT_191]'),CHAR(192),'[EXT_192]'),CHAR(193),'[EXT_193]'),CHAR(194),'[EXT_194]'),CHAR(195),'[EXT_195]'),CHAR(196),'[EXT_196]'),CHAR(197),'[EXT_197]')
		,CHAR(198),'[EXT_198]'),CHAR(199),'[EXT_199]'),CHAR(200),'[EXT_200]'),CHAR(201),'[EXT_201]'),CHAR(202),'[EXT_202]'),CHAR(203),'[EXT_203]'),CHAR(204),'[EXT_204]'),CHAR(205),'[EXT_205]'),CHAR(206),'[EXT_206]'),CHAR(207),'[EXT_207]')
		,CHAR(208),'[EXT_208]'),CHAR(209),'[EXT_209]'),CHAR(210),'[EXT_210]'),CHAR(211),'[EXT_211]'),CHAR(212),'[EXT_212]'),CHAR(213),'[EXT_213]'),CHAR(214),'[EXT_214]'),CHAR(215),'[EXT_215]'),CHAR(216),'[EXT_216]'),CHAR(217),'[EXT_217]')
		,CHAR(218),'[EXT_218]'),CHAR(219),'[EXT_219]'),CHAR(220),'[EXT_220]'),CHAR(221),'[EXT_221]'),CHAR(222),'[EXT_222]'),CHAR(223),'[EXT_223]'),CHAR(224),'[EXT_224]'),CHAR(225),'[EXT_225]'),CHAR(226),'[EXT_226]'),CHAR(227),'[EXT_227]')
		,CHAR(228),'[EXT_228]'),CHAR(229),'[EXT_229]'),CHAR(230),'[EXT_230]'),CHAR(231),'[EXT_231]'),CHAR(232),'[EXT_232]'),CHAR(233),'[EXT_233]'),CHAR(234),'[EXT_234]'),CHAR(235),'[EXT_235]'),CHAR(236),'[EXT_236]'),CHAR(237),'[EXT_237]')
		,CHAR(238),'[EXT_238]'),CHAR(239),'[EXT_239]'),CHAR(240),'[EXT_240]'),CHAR(241),'[EXT_241]'),CHAR(242),'[EXT_242]'),CHAR(243),'[EXT_243]'),CHAR(244),'[EXT_244]'),CHAR(245),'[EXT_245]'),CHAR(246),'[EXT_246]'),CHAR(247),'[EXT_247]')
		,CHAR(248),'[EXT_248]'),CHAR(249),'[EXT_249]'),CHAR(250),'[EXT_250]'),CHAR(251),'[EXT_251]'),CHAR(252),'[EXT_252]'),CHAR(253),'[EXT_253]'),CHAR(254),'[EXT_254]'),CHAR(255),'[EXT_255]');

	RETURN @Output;
END
GO
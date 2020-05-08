WHEN
	LOL_ST  = 'GA'     
	AND clstat.I_ALLCLM IS NULL
	AND 
	(
		MAIN.A_EST_LOSS = 0
		OR MAIN.A_STTLMT = 0
	) 
THEN
	'Estimated and/or Settlement Amount Missing'

WHEN 
	[LOL_ST] = 'KS'
	AND main.A_STTLMT > 0
	AND main.A_EST_LOSS = 0
	AND
	(
		clt1.T_LOSS_DSC IN
		(
			'FIRE',
			'BLANK'
		)
		OR clt1.T_LOSS_DSC_EXT IN
		(
			'FIRE',
			'',
			'BLANK'
		)
	)
THEN
	'Loss Description Invalid'
  
WHEN
	[LOL_ST] = 'KS'
	AND main.A_STTLMT = 0
	AND main.A_EST_LOSS > 0
	AND
	(
		clt1.T_LOSS_DSC IN
		(
			'FIRE',
			'BLANK'
		)
		OR clt1.T_LOSS_DSC_EXT IN
		(
			'FIRE',
			'',
			'BLANK'
		)
	)

THEN
	'Loss Description Invalid'

WHEN 
	(
		LOL_ST  = 'KS'
		AND
		(
			main.A_STTLMT > 0
			OR main.A_EST_LOSS > 0
		)
		AND
		(
			clt1.T_LOSS_DSC IN
			(	
				'FIRE',
				'BLANK'
			)
			OR
				clt1.T_LOSS_DSC_EXT IN
			(
				'FIRE',
				'BLANK'
			)
		)
	)
THEN
	'Loss Description Invalid'

WHEN 
	[LOL_ST] = 'KS'
	AND clt1.T_LOSS_DSC IN
	(
		'FIRE',
		'BLANK'
	)
	AND
	(
		main.A_STTLMT = 0
		AND main.A_EST_LOSS = 0
	) 
	OR clt1.T_LOSS_DSC_EXT IN
	(
		'FIRE',
		'BLANK'
	)
	AND
	(
		main.A_STTLMT = 0
		AND main.A_EST_LOSS = 0
	) 
THEN
	'Loss Description Invalid, Estimated and/or Settlement amount missing' 
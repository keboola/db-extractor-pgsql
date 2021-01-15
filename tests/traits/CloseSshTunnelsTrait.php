<?php

declare(strict_types=1);

namespace Keboola\DbExtractor\TraitTests;

use Symfony\Component\Process\Process;

trait CloseSshTunnelsTrait
{
    protected function closeSshTunnels(): void
    {
        # Close SSH tunnel if created
        $process = new Process(['sh', '-c', 'pgrep ssh | xargs -r kill']);
        $process->mustRun();
    }
}
